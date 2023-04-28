
import requests
import time

checkCertificate = False
pifopHost = "localhost:3445"

OperationSuccessEventType = {
    "function_initialization": "function_initialized",
    "execution_initialization": "execution_initialized",
    "input_upload": "input_uploaded",
    "execution_start": "execution_started",
    "execution_stop": "execution_stopped",
    "execution_info_retrieval": "execution_info",
    "output_retrieval": "output_retrieved",
    "execution_termination": "execution_terminated",
    "new_key": "new_key_created",
    "delete_key": "key_deleted"
}

def getElapsedTime(date):
    return time.time() - date

class Request:
    def __init__(self, endpoint, options):
        self.endpoint = endpoint
        self.options = options
        self.headers = None
        self.body = None
        self.method = requests.get
        self.attempts = 0
        self.maxAttempts = 6
        
        if "method" in options:
            if options["method"] == "POST":
                self.method = requests.post
            elif options["method"] == "DELETE":
                self.method = requests.delete
        
        if "headers" in options:
            self.headers = options["headers"]
            
        if "body" in options:
            self.body = options["body"]
        
    def send(self):
        self.attempts += 1
        return self.method(self.endpoint, headers=self.headers, data=self.body, verify=checkCertificate)

class Event:
    def __init__(self, operation, type, response, data, execution, func, objectType):
        self.operation = operation
        self.type = type
        self.response = response
        self.data = data
        self.execution = execution
        self.func = func
        self.objectType = objectType

def getObjectType(operation):
    executionOperations = ["execution_initialization", "input_upload", "execution_start", "execution_stop", "execution_info_retrieval", "output_retrieval", "execution_termination"]
    functionOperations = ["function_initialization"]
    apiKeyOperations = ["new_key_created"]
    
    if (operation in executionOperations):
        return "execution"
    if (operation in functionOperations):
        return "function"
    if (operation in apiKeyOperations):
        return "api_key"
    
    return None;
    
def getFuncIdFromUID(uid):
    array = uid.split("/")
    if (len(array) == 2):
        return array[1]
    
    return array[0];

def getFuncAuthorFromUID(uid):
    array = uid.split("/")
    if (len(array) == 2):
        return array[0]
    
    return None

def internalEventListener(event):
    print(f'{event.type}')
    
    execution = event.execution
    func = event.func
    ended = False

    if event.type == "function_initialized":
        func.config = event.data
        func.initialized = True
    elif event.type == "execution_initialized":
        execution.data = event.data
        execution.id = event.data["id"]
        execution.endpoint = f'https://{pifopHost}/{execution.func.author}/{execution.func.id}/executions/{execution.id}'
        execution.initialized = True
        execution.apiKey = func.apiKey
    elif event.type == "execution_started":
        execution.running = True
    elif event.type == "execution_info":
        execution.status = event.data["status"]
        execution.log += event.data["stdout"]
        if "output" in event.data:
            execution.generatedOutput = event.data["output"]
    

def handleResponse(operation, request, response, subject, outputId=None):
    execution = None
    func = None
    
    if (operation != "function_initialization" and operation != "new_key"):
        execution = subject
        func = execution.func
    else:
        func = subject
    
    successEventType = OperationSuccessEventType[operation]
    objectType = getObjectType(operation)
    
    if response.ok:
        if operation != "output_retrieval":
            data = response.json()
            event = Event(operation, successEventType, response, data, execution, func, objectType)
            internalEventListener(event)
        else:
            output = execution.getGeneratedOutput(outputId)
            output["blob"] = response.content
            
            if output["path"].endswith(".json") or output["path"].endswith(".csv") or output["path"].endswith(".txt"):
                output["text"] = response.text
                if output["path"].endswith(".json"):
                    output["json"] = response.json()
                
                event = Event(operation, successEventType, response, output, execution, func, objectType)
                internalEventListener(event)
            else:
                event = Event(operation, successEventType, response, output, execution, func, objectType)
                internalEventListener(event)
    else:
        pass

class Execution:
    def __init__(self, func):
        self.initialized = False
        self.ok = True
        self.endpoint = None
        self.eventListeners = []
        self.metadata = {}
        self.input = None
        self.providedInput = []
        self.nextInputToUpload = 0
        self.nextOutputToDownload = 0
        self.nextInfoRetrieval = None
        self.generatedOutput = []
        self.output = {}
        self.result = None
        self.stopped = False
        self.status = ""
        self.func = func
        self.apiKey = func.apiKey
        self.log = ""
        self.logIndex = 0
        self.lastInfoRetrieval = 0
        self.getHTTPHeaders = lambda: {"Authorization": f'Bearer {self.apiKey}'}
        self.pendingLog = lambda: self.logIndex < self.log
        
    def nextLog(self):
        result = self.log[self.logIndex:]
        self.logIndex = len(self.log)
        return result
        
    def setMetadata(self, key, value):
        self.metadata[key] = value
        return self
    
    def interrupt(self):
        self.stopped = True
        # TODO
        # if self.nextInfoRetrieval is not None:
        #     clearTimeout(self.nextInfoRetrieval)
        #     self.nextInfoRetrieval = None

    def uploadNextInput(self):
        if self.nextInputToUpload < len(self.providedInput):
            input = self.providedInput[self.nextInputToUpload]
            self.uploadInput(input['id'], input['content'])
            self.nextInputToUpload += 1
            return True
        else:
            return False

    def downloadNextOutput(self):
        if self.generatedOutput and self.nextOutputToDownload < len(self.generatedOutput):
            output = self.generatedOutput[self.nextOutputToDownload]
            self.downloadOutput(output['id'])
            self.nextOutputToDownload += 1
            return True
        else:
            return False

    def initialize(self):
        # POST func.pifop.com/:func_author/:func_id
        request = Request(f'{self.func.endpoint}/executions', {"method": "POST", "headers": self.getHTTPHeaders()})
        response = request.send()
        handleResponse("execution_initialization", request, response, self)
        return self

    def uploadInput(self, inputId, inputContent):
        if "input" not in self.func.config:
            ev = Event("input_upload", "error", Response("", status=400), {'errorMessage': 'This function does not accepts any inputs.'}, self, self.func, "execution")
            internalEventListener(ev)
            return self

        elif inputId == "":
            if len(self.func.config["input"]) >= 2:
                ev = Event("input_upload", "error", Response("", status=400), {'errorMessage': 'Unidentified input files can only be provided when the function being called expects a single input file, but the function you are calling accepts multiple input files. Use `setInput(id, content)` to specify the id of the file that you want to upload.'}, self, self.func, "execution")
                internalEventListener(ev)
                return self
            else:
                inputId = self.func.config["input"][0]['id']
        
        # POST func.pifop.com/$func_author/$func_id/executions/$exec_id/input/$input_id
        request = Request(f'{self.endpoint}/input/{inputId}', {"method": "POST", "headers": self.getHTTPHeaders(), "body": inputContent})
        response = request.send()
        handleResponse("input_upload", request, response, self)
        return self
    
    def downloadOutput(self, outputId):
        # GET func.pifop.com/$func_author/$func_id/executions/$exec_id/output/$output_id
        request = Request(f'{self.endpoint}/output/{outputId}', {"headers": self.getHTTPHeaders()})
        response = request.send()
        handleResponse("output_retrieval", request, response, self, outputId)

    def getGeneratedOutput(self, id):
        result = None
        for output in self.generatedOutput:
            if output["id"] == id:
                result = output
                break
        return result
    
    def start(self):
        # POST func.pifop.com/$func_author/$func_id/executions/$exec_id/start
        request = Request(f'{self.endpoint}/start', {"method": "POST", "headers": self.getHTTPHeaders()})
        response = request.send()
        handleResponse("execution_start", request, response, self)
        
    def waitCompletion(self):
        while self.isRunning():
            pass
        return self.ok
        
    def stop(self):
        self.interrupt()
        if not self.initialized:
            return
        
        # POST func.pifop.com/$func_author/$func_id/executions/$exec_id/stop
        request = Request(f'{self.endpoint}/stop', {"method": "POST", "headers": self.getHTTPHeaders()})
        response = request.send()
        handleResponse("execution_stop", request, response, self)
        
    def terminate(self):
        self.interrupt()
        if not self.initialized:
            return
        
        # DELETE func.pifop.com/$func_author/$func_id/executions/$exec_id
        request = Request(self.endpoint, {"method": "DELETE", "headers": self.getHTTPHeaders()})
        response = request.send()
        handleResponse("execution_termination", request, response, self)

    def getInfo(self):
        # GET func.pifop.com/$func_author/$func_id/executions/$exec_id?stdout=true
        request = Request(f'{self.endpoint}?stdout=true', {"headers": self.getHTTPHeaders()})
        response = request.send()
        handleResponse("execution_info_retrieval", request, response, self)
        self.lastInfoRetrieval = time.time()
    
    def isRunning(self):
        if self.status == "ended":
            for entry in self.generatedOutput:
                self.downloadOutput(entry["id"])
                
            if len(self.generatedOutput) == 1:
                if "json" in self.generatedOutput[0]:
                    self.result = self.generatedOutput[0]["json"]
                else:
                    self.result = self.output
            else:
                self.result = self.output
            
            self.terminate()
            
            return False
        else:
            sleepTime = 1 - getElapsedTime(self.lastInfoRetrieval)
            
            if sleepTime >= 0:
                time.sleep(sleepTime)
                
            self.getInfo()
            
            return True
        


class Function:
    def __init__(self, funcUID, apiKey, masterKey=None):
        self.initialized = False
        self.author = getFuncAuthorFromUID(funcUID)
        self.id = getFuncIdFromUID(funcUID)
        self.endpoint = f'https://{pifopHost}/{self.author}/{self.id}'
        self.apiKey = apiKey
        self.masterKey = masterKey
        self.eventListeners = []
        self.executions = []
        self.config = None
        self.getHTTPHeaders = lambda: {"Authorization": f'Bearer {self.apiKey}'}
        
    def initialize(self):
        # GET func.pifop.com/:func_author/:func_id
        request = Request(self.endpoint, {"headers": self.getHTTPHeaders()})
        response = request.send()
        handleResponse("function_initialization", request, response, self)
        
    def execute(self):
        execution = Execution(self)
        execution.initialize()
        return execution
    
    def genAPIKey(self, name):
        # POST func.pifop.com/:func_author/:func_id/api_keys?name=:key_name&max_memory=10
        request = Request(f'{self.endpoint}/api_keys?name={name}', {"method": "POST", "headers": {"Authorization": f'Bearer {self.masterKey}'}})
        response = request.send()
        handleResponse("new_key", request, response, self)
        
    def deleteAPIKey(self, keyName):
        # DELETE func.pifop.com/:func_author/:func_id/api_keys/$key_name
        request = Request(f'{self.endpoint}/api_keys/{keyName}', {"method": "DELETE", "headers": {"Authorization": f'Bearer {self.masterKey}'}})
        response = request.send()
        handleResponse("delete_key", request, response, self)


def execute(funcUID, apiKey, input=None):
    func = Function(funcUID, apiKey)
    func.initialize()
    execution = func.execute()
    
    if input:
        execution.uploadInput("", input)
        execution.start()
    
    return execution


