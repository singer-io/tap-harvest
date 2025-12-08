class HarvestError(Exception):
    """Class representing Generic Http error."""

    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response


class HarvestBackoffError(HarvestError):
    """Class representing backoff error handling."""

    pass


class HarvestBadRequestError(HarvestError):
    """Class representing 400 status code."""

    pass


class HarvestUnauthorizedError(HarvestError):
    """Class representing 401 status code."""

    pass


class HarvestForbiddenError(HarvestError):
    """Class representing 403 status code."""

    pass


class HarvestNotFoundError(HarvestError):
    """Class representing 404 status code."""

    pass


class HarvestConflictError(HarvestError):
    """Class representing 406 status code."""

    pass


class HarvestUnprocessableEntityError(HarvestBackoffError):
    """Class representing 409 status code."""

    pass


class HarvestRateLimitError(HarvestBackoffError):
    """Class representing 429 status code."""

    pass


class HarvestInternalServerError(HarvestBackoffError):
    """Class representing 500 status code."""

    pass


class HarvestNotImplementedError(HarvestBackoffError):
    """Class representing 501 status code."""

    pass


class HarvestBadGatewayError(HarvestBackoffError):
    """Class representing 502 status code."""

    pass


class HarvestServiceUnavailableError(HarvestBackoffError):
    """Class representing 503 status code."""

    pass


ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": HarvestBadRequestError,
        "message": "A validation exception has occurred.",
    },
    401: {
        "raise_exception": HarvestUnauthorizedError,
        "message": "The access token provided is expired, revoked, malformed or invalid for other reasons.",
    },
    403: {
        "raise_exception": HarvestForbiddenError,
        "message": "You are missing the following required scopes: read",
    },
    404: {
        "raise_exception": HarvestNotFoundError,
        "message": "The resource you have specified cannot be found.",
    },
    409: {
        "raise_exception": HarvestConflictError,
        "message": "The API request cannot be completed because the requested operation would conflict with an existing item.",
    },
    422: {
        "raise_exception": HarvestUnprocessableEntityError,
        "message": "The request content itself is not processable by the server.",
    },
    429: {
        "raise_exception": HarvestRateLimitError,
        "message": "The API rate limit for your organisation/application pairing has been exceeded.",
    },
    500: {
        "raise_exception": HarvestInternalServerError,
        "message": "The server encountered an unexpected condition which prevented"
        " it from fulfilling the request.",
    },
    501: {
        "raise_exception": HarvestNotImplementedError,
        "message": "The server does not support the functionality required to fulfill the request.",
    },
    502: {
        "raise_exception": HarvestBadGatewayError,
        "message": "Server received an invalid response.",
    },
    503: {
        "raise_exception": HarvestServiceUnavailableError,
        "message": "API service is currently unavailable.",
    },
}
