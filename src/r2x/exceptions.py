"""Custom exceptions for R2X."""
# ruff: noqa: D101


class DuplicateUUIDError(Exception):
    pass


class DuplicateNameError(Exception):
    pass


class NotFoundError(Exception):
    pass


class MultlipleElementsError(Exception):
    pass


class ModelError(Exception):
    pass


class MultipleFilesError(Exception):
    pass


class ParserError(Exception):
    pass
