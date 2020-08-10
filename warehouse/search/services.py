from warehouse.search.interfaces import ISearchService
from zope.interface import implementer

@implementer(ISearchService)
class ElasticSearchService:
    def search(query):
        print("*** We are using ElasticSearch to search:", query)

@implementer(ISearchService)
class MeiliSearchService:
    def search(query):
        print("*** We are using MeiliSearch to search:", query)