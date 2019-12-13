from django.conf import settings
from django.db.models import Case, When

from elasticsearch_dsl import Search as DSLSearch


class Search(DSLSearch):
    def __init__(self, **kwargs):
        self._model = kwargs.pop('model', None)
        super(Search, self).__init__(**kwargs)

    def _clone(self):
        s = super(Search, self)._clone()
        s._model = self._model
        return s

    def to_queryset(self, keep_order=True):
        """
        This method return a django queryset from the an elasticsearch result.
        It cost a query to the sql db.
        """
        s = self

        # Do not query again if the es result is already cached
        if not hasattr(self, '_response'):
            # We only need the meta fields with the models ids
            s = self.source(excludes=['*'])
            s = s.execute()

        pks = [result.meta.id for result in s]

        qs = self._model.objects.filter(pk__in=pks)

        if keep_order:
            preserved_order = Case(
                *[When(pk=pk, then=pos) for pos, pk in enumerate(pks)]
            )
            qs = qs.order_by(preserved_order)

        return qs

    def to_dict(self, count=False, **kwargs):
        query_body = super(Search, self).to_dict(count=count, **kwargs)
        if not count:
            if 'from' not in query_body:
                query_body['from'] = 0
            if 'size' not in query_body:
                query_body['size'] = getattr(settings, 'ELASTICSEARCH_DSL_LOAD_PER_QUERY', 10)
        return query_body


class SearchNone(Search):
    def __init__(self, **kwargs):
        super(SearchNone, self).__init__(**kwargs)
        self._response = self._response_class(
            self,
            {
                'hits': {
                    'hits': [],
                    'total': 0,
                    'max_score': None
                }
            }
        )
