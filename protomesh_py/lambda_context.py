from grpc import ServicerContext, StatusCode
from typing import Dict, List
from aws_lambda_typing import context as context_, events, responses
from lambda_conversion import convert_grpc_to_http_status_code

class GrpcContext(ServicerContext):

    __client_metadata: Dict[str, List[str]] = None
    __code: StatusCode = StatusCode.OK
    __details: str = None
    __service_metadata: Dict[str, List[str]] = {}

    def __init__(self, event: events.APIGatewayProxyEventV1, context: context_.Context):
        
        self.__client_metadata = event["headers"].copy()

        for key, value in event["multiValueHeaders"].items():
            self.__client_metadata[key] = [value]

    def invocation_metadata(self) -> Dict[str, List[str]]:
        return self.__client_metadata
    
    def abort(self, code, details):
        self.__code = code
        self.__details = details
    
    def set_code(self, code):
        self.__code = code

    def set_details(self, details):
        self.__details = details

    def code(self):
        return self.__code
    
    def details(self):
        return self.__details
    
    def set_trailing_metadata(self, trailing_metadata):
        self.__service_metadata = trailing_metadata

    def trailing_metadata(self):
        return self.__service_metadata
    
    def attach_to_response(self, res: responses.APIGatewayProxyResponseV1):

        res["statusCode"] = convert_grpc_to_http_status_code(self.__code).value

        for key, value in self.__service_metadata.items():
            res["multiValueHeaders"][key] = value
