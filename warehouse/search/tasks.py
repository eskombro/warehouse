# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
from warehouse.search import MeiliSearchService


def _project_docs(db, project_name=None):

    releases_list = (
        db.query(Release.id)
        .filter(Release.yanked.is_(False), Release.files)
        .order_by(
            Release.project_id,
            Release.is_prerelease.nullslast(),
            Release._pypi_ordering.desc(),
        )
        .distinct(Release.project_id)
    )

    if project_name:
        releases_list = releases_list.join(Project).filter(Project.name == project_name)

    releases_list = releases_list.subquery()

    r = aliased(Release, name="r")

    all_versions = (
        db.query(func.array_agg(r.version))
        .filter(r.project_id == Release.project_id)
        .correlate(Release)
        .as_scalar()
        .label("all_versions")
    )

    classifiers = (
        db.query(func.array_agg(Classifier.classifier))
        .select_from(release_classifiers)
        .join(Classifier, Classifier.id == release_classifiers.c.trove_id)
        .filter(Release.id == release_classifiers.c.release_id)
        .correlate(Release)
        .as_scalar()
        .label("classifiers")
    )

    release_data = (
        db.query(
            Description.raw.label("description"),
            Release.version.label("latest_version"),
            all_versions,
            Release.author,
            Release.author_email,
            Release.maintainer,
            Release.maintainer_email,
            Release.home_page,
            Release.summary,
            Release.keywords,
            Release.platform,
            Release.download_url,
            Release.created,
            classifiers,
            Project.normalized_name,
            Project.name,
            Project.zscore,
        )
        .select_from(releases_list)
        .join(Release, Release.id == releases_list.c.id)
        .join(Description)
        .outerjoin(Release.project)
    )

    for release in windowed_query(release_data, Release.project_id, 50000):
        p = ProjectDocument.from_db(release)
        p._index = None
        p.full_clean()
        doc = p.to_dict(include_meta=True)
        doc.pop("_index", None)
        yield doc


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


@tasks.task(bind=True, ignore_result=True, acks_late=True)
def reindex(self, request):
    """
    Recreate the Search Index.
    """

    search_backend = request.find_service(ISearchService, context=None)
    if search_backend.__name__ == 'MeiliSearchService':
        print("Reindex all content in MeiliSearch")
    elif search_backend.__name__ == 'ElasticSearchService':
        print("Reindex all content in ElasticSearch")
    return search_backend.reindex(request, _project_docs(request.db))
    
    


@tasks.task(bind=True, ignore_result=True, acks_late=True)
def reindex_project(self, request, project_name):
    """
    Reindex a single project
    """

    search_backend = request.find_service(ISearchService, context=None)
    if search_backend.__name__ == 'MeiliSearchService':
        print("Reindex a single project in MeiliSearch")
    elif search_backend.__name__ == 'ElasticSearchService':
        print("Reindex a single project in ElasticSearch")
    search_backend.reindex_project(request, project_name)


@tasks.task(bind=True, ignore_result=True, acks_late=True)
def unindex_project(self, request, project_name):
    r = redis.StrictRedis.from_url(request.registry.settings["celery.scheduler_url"])
    try:
        with SearchLock(r, timeout=15, blocking_timeout=1):
            client = request.registry["elasticsearch.client"]
            index_name = request.registry["elasticsearch.index"]
            try:
                client.delete(index=index_name, doc_type="doc", id=project_name)
            except elasticsearch.exceptions.NotFoundError:
                pass
    except redis.exceptions.LockError as exc:
        raise self.retry(countdown=60, exc=exc)
