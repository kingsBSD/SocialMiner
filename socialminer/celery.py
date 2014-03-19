from __future__ import absolute_import
# Licensed under the Apache License Version 2.0: http://www.apache.org/licenses/LICENSE-2.0.txt

from celery import Celery


# Rigmarole if you want proper docstrings for tasks.
# https://github.com/celery/celery/issues/1636
#def _default_cls_attr(name, type_, cls_value):
#    # Stolen from celery's proxy objects

#    def __new__(cls, getter):
#        instance = type_.__new__(cls, cls_value)
#        instance.__getter = getter
#        return instance

#    def __get__(self, obj, cls=None):
#        return self.__getter(obj) if obj is not None else self

#    def __set__(self, obj, value):
#        raise AttributeError('readonly attribute')

#    return type(name, (type_, ), {
#        '__new__': __new__, '__get__': __get__, '__set__': __set__,
#    })


#class surface_docstring(object):
#    def __init__(self, func):
#        self.func = func

#    @_default_cls_attr('doc', str, __doc__)
#    def __doc__(self):
#        return self.func._get_current_object().__doc__

#    def __getattr__(self, attr):
#        return getattr(self.func, attr)

app = Celery('socialminer', broker='amqp://guest@localhost//', backend='redis://localhost:6379', 
	include=['socialminer.twitter_tasks'])

app.conf.update(
    CELERY_TASK_SERIALIZER = "json",
    CELERYD_CONCURRENCY = 4
)

if __name__ == '__main__':
    app.start()
