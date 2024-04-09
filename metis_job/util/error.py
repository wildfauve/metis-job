from metis_fn import monad, fn


class BaseError(Exception):

    def __init__(self,
                 message="",
                 name="",
                 ctx={},
                 request_kwargs: dict = {},
                 traceback=None,
                 code=500,
                 klass="",
                 retryable=False):
        self.code = 500 if code is None else code
        self.retryable = retryable
        self.message = message
        self.name = name
        self.ctx = ctx
        self.klass = klass
        self.traceback = traceback
        self.request_kwargs = request_kwargs
        super().__init__(self.message)

    def error(self):
        return {'error': self.message, 'code': self.code, 'step': self.name, 'ctx': self.ctx}

    def print(self):
        print(f"{self.message}\n\n{self.traceback}")
    ...


class MonadicErrorAggregate:

    def __init__(self, monadic_errors: list[monad.Either[BaseError, None]]):
        self.error_collection: list[BaseError] = self._filter_rights(monadic_errors)

    def _filter_rights(self, errors):
        return list(map(lambda er: er.error(), fn.select(monad.maybe_value_fail, errors)))

    def error(self):
        return {
            'error': "; ".join([err.message for err in self.error_collection]),
            'code': "; ".join([str(err.code) for err in self.error_collection]),
            'ctx': [err.ctx for err in self.error_collection]
        }

    def print(self):
        print(f"Number of Errors: {len(self.error_collection)}\n\n")
        for i, err in enumerate(self.error_collection):
            print(f"Error: {i}\n========\n")
            print(f"{err.message}\n\n{err.traceback}\n\n")



class InitialisationError(BaseError):
    ...

class RepoConfigError(BaseError):
    ...

class VocabNotFound(BaseError):
    ...

class SchemaMatchingError(BaseError):
    ...