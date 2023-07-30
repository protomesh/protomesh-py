from grpc import StatusCode
from http import HTTPStatus

def convert_grpc_to_http_status_code(grpc_status: StatusCode) -> HTTPStatus:

    match grpc_status:

        case StatusCode.OK:
            return HTTPStatus.OK

        case StatusCode.INVALID_ARGUMENT:
            return HTTPStatus.BAD_REQUEST
        
        case StatusCode.NOT_FOUND:
            return HTTPStatus.NOT_FOUND
        
        case StatusCode.ALREADY_EXISTS:
            return HTTPStatus.CONFLICT
        
        case StatusCode.PERMISSION_DENIED:
            return HTTPStatus.FORBIDDEN
        
        case StatusCode.UNAUTHENTICATED:
            return HTTPStatus.UNAUTHORIZED
        
        case StatusCode.RESOURCE_EXHAUSTED:
            return HTTPStatus.TOO_MANY_REQUESTS
        
        case StatusCode.FAILED_PRECONDITION:
            return HTTPStatus.PRECONDITION_FAILED
        
        case StatusCode.ABORTED:
            return HTTPStatus.CONFLICT
        
        case StatusCode.OUT_OF_RANGE:
            return HTTPStatus.BAD_REQUEST
        
        case StatusCode.UNIMPLEMENTED:
            return HTTPStatus.NOT_IMPLEMENTED
        
        case StatusCode.INTERNAL:
            return HTTPStatus.INTERNAL_SERVER_ERROR
        
        case StatusCode.UNAVAILABLE:
            return HTTPStatus.SERVICE_UNAVAILABLE
        
        case StatusCode.DATA_LOSS:
            return HTTPStatus.INTERNAL_SERVER_ERROR
        
        case _:
            raise Exception("Unknown status code")