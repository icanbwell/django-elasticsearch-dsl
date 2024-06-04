from __future__ import unicode_literals

from django.conf import settings
from django.core.paginator import Paginator
from django.db import models
from django.utils import translation
from elasticsearch.helpers import bulk
from elasticsearch_dsl import Document as DSLDocument
from six import iteritems

from .exceptions import ModelFieldNotMappedError
from .fields import (
    BooleanField,
    DateField,
    DEDField,
    DoubleField,
    FileField,
    IntegerField,
    KeywordField,
    LongField,
    ShortField,
    TextField,
)
from .search import Search, SearchNone

model_field_class_to_field_class = {
    models.AutoField: IntegerField,
    models.BigIntegerField: LongField,
    models.BooleanField: BooleanField,
    models.CharField: TextField,
    models.DateField: DateField,
    models.DateTimeField: DateField,
    models.EmailField: TextField,
    models.FileField: FileField,
    models.FilePathField: KeywordField,
    models.FloatField: DoubleField,
    models.ImageField: FileField,
    models.IntegerField: IntegerField,
    models.NullBooleanField: BooleanField,
    models.PositiveIntegerField: IntegerField,
    models.PositiveSmallIntegerField: ShortField,
    models.SlugField: KeywordField,
    models.SmallIntegerField: ShortField,
    models.TextField: TextField,
    models.TimeField: LongField,
    models.URLField: TextField,
}


class DocType(DSLDocument):
    def __init__(self, related_instance_to_ignore=None, **kwargs):
        super(DocType, self).__init__(**kwargs)
        self._related_instance_to_ignore = related_instance_to_ignore

    def __eq__(self, other):
        return id(self) == id(other)

    def __hash__(self):
        return id(self)

    @classmethod
    def _format_index_language(cls, index, language):
        return '{0}-{1}'.format(index, language)

    @classmethod
    def get_custom_index_name(cls, index, language=None):
        """
        Helper function to fetch the index name at this env
        """
        language_dsl_enabled = getattr(settings, 'ELASTICSEARCH_DSL_TRANSLATION_ENABLED', False)
        language = language or settings.LANGUAGE_ENGLISH
        index_prefix = getattr(settings, 'ES_INDEX_PREFIX', '')
        index_suffix = getattr(settings, 'ES_INDEX_SUFFIX', '')
        if isinstance(index, (list, tuple)):
            custom_indexes = []
            for i in index:
                custom_index = '{prefix}{index}{language}{suffix}'.format(
                    prefix=f'{index_prefix}-' if index_prefix else '',
                    index=i,
                    language=f'-{language}' if language_dsl_enabled else '',
                    suffix=f'-{index_suffix}' if index_suffix else ''
                )
                custom_indexes.append(custom_index)
            index = custom_indexes
        else:
            index = '{prefix}{index}{language}{suffix}'.format(
                prefix=f'{index_prefix}-' if index_prefix else '',
                index=i,
                language=f'-{language}' if language_dsl_enabled else '',
                suffix=f'-{index_suffix}' if index_suffix else ''
            )
        if not (language_dsl_enabled and index_prefix and index_suffix):
            return cls._default_index(index)
        return index

    @classmethod
    def search(cls, using=None, index=None):
        return Search(
            using=cls._get_using(using),
            index=cls.get_custom_index_name(index or cls.Index.name, language=translation.get_language()),
            doc_type=[cls],
            model=cls.django.model
        )

    @classmethod
    def none(cls, using=None, index=None):
        return SearchNone(
            using=cls._get_using(using),
            index=cls.get_custom_index_name(index or cls.Index.name, language=translation.get_language()),
            doc_type=[cls],
            model=cls.django.model
        )

    def translate_field(self, field_name, value, fail_silently=True):
        """
        Method to translate value of a field based on language
        :param field_name: Name of the field to be translated
        :param value: Value to be translated
        :param fail_silently: Whether to raise error when encountered
        :return: Translated value
        """
        return value

    def get_queryset(self):
        """
        Return the queryset that should be indexed by this doc type.
        """
        return self.django.model._default_manager.all()

    def prepare(self, instance, fail_silently=True):
        """
        Take a model instance, and turn it into a dict that can be serialized
        based on the fields defined on this DocType subclass
        """
        data = {}
        for name, field in iteritems(self._fields):
            if not isinstance(field, DEDField):
                continue

            if field._path == []:
                field._path = [name]

            prep_func = getattr(self, 'prepare_%s_with_related' % name, None)
            if prep_func:
                field_value = prep_func(
                    instance,
                    related_to_ignore=self._related_instance_to_ignore
                )
            else:
                prep_func = getattr(self, 'prepare_%s' % name, None)
                if prep_func:
                    field_value = prep_func(instance)
                else:
                    field_value = field.get_value_from_instance(
                        instance, self._related_instance_to_ignore
                    )

            if getattr(settings, 'ELASTICSEARCH_DSL_TRANSLATION_ENABLED', False):
                field_value = self.translate_field(name, field_value, fail_silently=fail_silently)

            data[name] = field_value

        return data

    @classmethod
    def to_field(cls, field_name, model_field):
        """
        Returns the elasticsearch field instance appropriate for the model
        field class. This is a good place to hook into if you have more complex
        model field to ES field logic
        """
        try:
            return model_field_class_to_field_class[
                model_field.__class__](attr=field_name)
        except KeyError:
            raise ModelFieldNotMappedError(
                "Cannot convert model field {} "
                "to an Elasticsearch field!".format(field_name)
            )

    def bulk(self, actions, **kwargs):
        # Executing bulk operation on all other clusters except default.
        # Here in action along with type of action, index name & it's id is provided.
        # We are explicitly updating this index of specified id for all clusters/clients
        for connection_alias in settings.ELASTICSEARCH_DSL.keys():
            (
                bulk(client=self._get_connection(using=connection_alias), actions=actions, **kwargs)
                if connection_alias != 'default' else None
            )
        # Handled default case separately to return output in case of default
        return bulk(client=self._get_connection(), actions=actions, **kwargs)

    def delete(self, **kwargs):
        # Executing delete operation on all available clusters
        for connection_alias in settings.ELASTICSEARCH_DSL.keys():
            super().delete(using=connection_alias, **kwargs)

    def save(self, **kwargs):
        # Executing save operation on all other clusters except default
        for connection_alias in settings.ELASTICSEARCH_DSL.keys():
            super().save(using=connection_alias, **kwargs) if connection_alias != 'default' else None

        return super().save(**kwargs)

    def _prepare_action(self, object_instance, action, language=None, fail_silently=True):
        with translation.override(language):
            return {
                '_op_type': action,
                '_index': self.get_custom_index_name(self._index._name, language),
                '_type': self._doc_type.name,
                '_id': object_instance.pk,
                '_source': (
                    self.prepare(object_instance, fail_silently) if action != 'delete' else None
                ),
            }

    def _get_actions(self, object_list, action, fail_silently=True):
        if getattr(settings, 'ELASTICSEARCH_DSL_TRANSLATION_ENABLED', False):
            if self.django.queryset_pagination is not None:
                paginator = Paginator(
                    object_list, self.django.queryset_pagination
                )
                for page in paginator.page_range:
                    for object_instance in paginator.page(page).object_list:
                        for language in settings.LANGUAGE_ANALYSERS:
                            yield self._prepare_action(
                                object_instance, action, language=language, fail_silently=fail_silently
                            )
            else:
                for object_instance in object_list:
                    for language in settings.LANGUAGE_ANALYSERS:
                        yield self._prepare_action(
                            object_instance, action, language=language, fail_silently=fail_silently
                        )
        else:
            if self.django.queryset_pagination is not None:
                paginator = Paginator(
                    object_list, self.django.queryset_pagination
                )
                for page in paginator.page_range:
                    for object_instance in paginator.page(page).object_list:
                        yield self._prepare_action(object_instance, action)
            else:
                for object_instance in object_list:
                    yield self._prepare_action(object_instance, action)

    def update(self, thing, refresh=None, action='index', **kwargs):
        """
        Update each document in ES for a model, iterable of models or queryset
        """
        fail_silently = kwargs.pop('fail_silently', True)
        if refresh is True or (
            refresh is None and self.django.auto_refresh
        ):
            kwargs['refresh'] = True

        if isinstance(thing, models.Model):
            object_list = [thing]
        else:
            object_list = thing

        return self.bulk(
            self._get_actions(object_list, action, fail_silently),
            chunk_size=getattr(settings, 'ELASTICSEARCH_DSL_CHUNK_SIZE', 500),
            max_chunk_bytes=getattr(settings, 'ELASTICSEARCH_DSL_CHUNK_BYTES', 100 * 1024 * 1024),
            **kwargs
        )


# Alias of DocType. Need to remove DocType in 7.x
Document = DocType
