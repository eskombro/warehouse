from zope.interface import implementer
import meilisearch

import binascii
import os
import urllib

import certifi
import elasticsearch
import redis
import requests_aws4auth

from elasticsearch.helpers import parallel_bulk
from elasticsearch_dsl import serializer
from sqlalchemy import func
from sqlalchemy.orm import aliased

from warehouse import tasks
from warehouse.packaging.models import (
    Classifier,
    Description,
    Project,
    Release,
    release_classifiers,
)
from warehouse.packaging.search import Project as ProjectDocument
from warehouse.search.utils import get_index
from warehouse.utils.db import windowed_query
from warehouse.search.interfaces import ISearchService


@implementer(ISearchService)
class ElasticSearchService:

    def search(query):
        print("*** We are using ElasticSearch to search:", query)

    def reindex(request, projects):
        r = redis.StrictRedis.from_url(request.registry.settings["celery.scheduler_url"])
        try:
            with SearchLock(r, timeout=30 * 60, blocking_timeout=30):
                p = urllib.parse.urlparse(request.registry.settings["elasticsearch.url"])
                qs = urllib.parse.parse_qs(p.query)
                kwargs = {
                    "hosts": [urllib.parse.urlunparse(p[:2] + ("",) * 4)],
                    "verify_certs": True,
                    "ca_certs": certifi.where(),
                    "timeout": 30,
                    "retry_on_timeout": True,
                    "serializer": serializer.serializer,
                }
                aws_auth = bool(qs.get("aws_auth", False))
                if aws_auth:
                    aws_region = qs.get("region", ["us-east-1"])[0]
                    kwargs["connection_class"] = elasticsearch.RequestsHttpConnection
                    kwargs["http_auth"] = requests_aws4auth.AWS4Auth(
                        request.registry.settings["aws.key_id"],
                        request.registry.settings["aws.secret_key"],
                        aws_region,
                        "es",
                    )
                client = elasticsearch.Elasticsearch(**kwargs)
                number_of_replicas = request.registry.get("elasticsearch.replicas", 0)
                refresh_interval = request.registry.get("elasticsearch.interval", "1s")

                # We use a randomly named index so that we can do a zero downtime reindex.
                # Essentially we'll use a randomly named index which we will use until all
                # of the data has been reindexed, at which point we'll point an alias at
                # our randomly named index, and then delete the old randomly named index.

                # Create the new index and associate all of our doc types with it.
                index_base = request.registry["elasticsearch.index"]
                random_token = binascii.hexlify(os.urandom(5)).decode("ascii")
                new_index_name = "{}-{}".format(index_base, random_token)
                doc_types = request.registry.get("search.doc_types", set())
                shards = request.registry.get("elasticsearch.shards", 1)

                # Create the new index with zero replicas and index refreshes disabled
                # while we are bulk indexing.
                new_index = get_index(
                    new_index_name,
                    doc_types,
                    using=client,
                    shards=shards,
                    replicas=0,
                    interval="-1",
                )
                new_index.create(wait_for_active_shards=shards)

                # From this point on, if any error occurs, we want to be able to delete our
                # in progress index.
                try:
                    request.db.execute("SET statement_timeout = '600s'")

                    for _ in parallel_bulk(
                        client, projects, index=new_index_name
                    ):
                        pass
                except:  # noqa
                    new_index.delete()
                    raise
                finally:
                    request.db.rollback()
                    request.db.close()

                # Now that we've finished indexing all of our data we can update the
                # replicas and refresh intervals.
                client.indices.put_settings(
                    index=new_index_name,
                    body={
                        "index": {
                            "number_of_replicas": number_of_replicas,
                            "refresh_interval": refresh_interval,
                        }
                    },
                )

                # Point the alias at our new randomly named index and delete the old index.
                if client.indices.exists_alias(name=index_base):
                    to_delete = set()
                    actions = []
                    for name in client.indices.get_alias(name=index_base):
                        to_delete.add(name)
                        actions.append({"remove": {"index": name, "alias": index_base}})
                    actions.append({"add": {"index": new_index_name, "alias": index_base}})
                    client.indices.update_aliases({"actions": actions})
                    client.indices.delete(",".join(to_delete))
                else:
                    client.indices.put_alias(name=index_base, index=new_index_name)
        except redis.exceptions.LockError as exc:
            raise self.retry(countdown=60, exc=exc)
    
    def reindex_project(request, project_name):
        r = redis.StrictRedis.from_url(request.registry.settings["celery.scheduler_url"])
        try:
            with SearchLock(r, timeout=15, blocking_timeout=1):
                client = request.registry["elasticsearch.client"]
                doc_types = request.registry.get("search.doc_types", set())
                index_name = request.registry["elasticsearch.index"]
                get_index(
                    index_name,
                    doc_types,
                    using=client,
                    shards=request.registry.get("elasticsearch.shards", 1),
                    replicas=request.registry.get("elasticsearch.replicas", 0),
                )

                for _ in parallel_bulk(
                    client, _project_docs(request.db, project_name), index=index_name
                ):
                    pass
        except redis.exceptions.LockError as exc:
            raise self.retry(countdown=60, exc=exc)

@implementer(ISearchService)
class MeiliSearchService:

    def search(query):
        print("*** We are using MeiliSearch to search:", query)

    def reindex(request, projects):
        print("*** We are reindexing projects in MeiliSearch")
        ms_client = meilisearch.Client('http://meilisearch:7700')
        try: 
            index = ms_client.get_index('projects').delete()
        except meilisearch.errors.MeiliSearchApiError as e:
            if e.error_code == 'index_not_found':
                pass
        index = ms_client.get_or_create_index('projects', {'primaryKey': '_id'})
        projects_list = []
        for p in projects:
            p['_source']['_id'] = p['_id']
            p['_source']['created'] = str(p['_source']['created'])
            projects_list.append(p['_source'])
            if len(projects_list) >= 1000:
                index.add_documents(projects_list)
                projects_list = []
        index.add_documents(projects_list)
    
    def reindex_project(request, project_name):
        pass


class SearchLock:
    def __init__(self, redis_client, timeout=None, blocking_timeout=None):
        self.lock = redis_client.lock(
            "search-index", timeout=timeout, blocking_timeout=blocking_timeout
        )

    def __enter__(self):
        if self.lock.acquire():
            return self
        else:
            raise redis.exceptions.LockError("Could not acquire lock!")

    def __exit__(self, type, value, tb):
        self.lock.release()