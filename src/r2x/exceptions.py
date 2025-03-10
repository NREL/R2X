"""Custom exceptions for R2X."""
# ruff: noqa: D101


class R2XDuplicateUUIDError(Exception):
    pass


class R2XDuplicateNameError(Exception):
    pass


class R2XNotFoundError(Exception):
    pass


class R2XMultlipleElementsError(Exception):
    pass


class R2XModelError(Exception):
    pass


class R2XMultipleFilesError(Exception):
    pass


class R2XParserError(Exception):
    pass


class R2XFieldRemovalError(Exception):
    pass
