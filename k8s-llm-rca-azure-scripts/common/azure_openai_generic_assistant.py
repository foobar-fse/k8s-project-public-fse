#!/usr/bin/env python


import os
import openai
#from openai import OpenAI
from openai import AzureOpenAI
import time


class OpenAIGenericAssistant:
    def __init__(self):
        # Initialize the OpenAI client with the API key from environment variable
        #openai.api_key = os.getenv("OPENAI_API_KEY")
        #self.client = OpenAI()
        
        self.client = AzureOpenAI(
                    api_key=os.environ["AZURE_OPENAI_API_KEY"],
                    azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
                    api_version="2024-05-01-preview",
                    )
        # initialize counter
        self.counter = 0

    def create_assistant(self, instructions, name, model='foobar-gpt-4o'): # replace with your deployment in azure
        # Create an Assistant
        self.assistant = self.client.beta.assistants.create(
                instructions=instructions,
                name=name,
                model=model,
                #tools=[{"type": "code_interpreter"}]
        )
        # Set message counter 
        self.counter = 0

    def retrieve_assistant(self, assistant_id):
        # Retrive an existing Assistant
        self.assistant = self.client.beta.assistants.retrieve(assistant_id)

    def create_thread(self):
        # Create a Thread
        self.thread = self.client.beta.threads.create()

    def retrieve_thread(self, thread_id):
        # Retrieve an existing Thread
        self.thread = self.client.beta.threads.retrieve(thread_id)
    
    def add_message(self, content):
        # Add a Message to a Thread
        self.message = self.client.beta.threads.messages.create(
            thread_id=self.thread.id,
            role="user",
            content=content
        )
        # Increase message counter
        self.counter += 1 

    def run_assistant(self, instructions=None):
        # Run the Assistant
        self.run = self.client.beta.threads.runs.create(
            thread_id=self.thread.id,
            assistant_id=self.assistant.id,
            instructions=instructions
        )

    def get_run_status(self):
        # Check the Run status
        return self.client.beta.threads.runs.retrieve(
            thread_id=self.thread.id,
            run_id=self.run.id
        )

    def display_response(self):
        # Display the Assistant's Response
        messages = self.client.beta.threads.messages.list(
            thread_id=self.thread.id,
            limit = 1
        )
        # only show the latest message
        print(messages.data[0])

    def get_last_message(self):
        # get the last message, new added
        messages = self.client.beta.threads.messages.list(
            thread_id=self.thread.id,
            limit = 1
        )
        return messages

    def get_all_message(self):
        # get about all messages in the response, about 20 messages
        messages = self.client.beta.threads.messages.list(
            thread_id=self.thread.id
        )
        return messages

   
    def get_last_k_message(self, num):
        messages = self.client.beta.threads.messages.list(
            thread_id=self.thread.id,
            limit = num
        )
        return messages

    def wait_get_last_k_message(self, num=1):
        # wait and get the last message
        maxPolling = 120
        period = 5
        for i in range(1, maxPolling+1):
            time.sleep(i * period)
            print('polling run result after %d seconds' % (i * period))
            run = self.get_run_status()
            if(run.status == 'completed'):
                print('run completed')
                # Increase message counter by 1, the assistant response with 1 message? YES
                self.counter += 1
                messages = self.get_last_k_message(num)
                return messages
            elif (run.status == 'cancelled'):
                print('run cancelled')
                return None
            elif (run.status == 'failed'):
                print('run failed')
                return None
            elif (run.status == 'expired'):
                print('run expired')
                return None
            elif (i == maxPolling):
                print ('last polling and time out in %d seconds' % period * maxPolling)
                return None

    def get_token_usage(self, tmin, tmax, limit=20):
        # get the token usage in [tmin, tmax)
        runs = self.client.beta.threads.runs.list(
                thread_id = self.thread.id,
                order = 'desc',
                limit = limit,
                )
        
        token_usage = {'prompt_tokens': 0, 'completion_tokens': 0, 'total_tokens': 0}

        for run in runs:
            if (run.created_at != None) and (run.completed_at != None) and \
                (run.created_at >= tmin) and (run.created_at < tmax) and \
                (run.completed_at >= tmin) and (run.completed_at < tmax):
                token_usage['prompt_tokens'] += run.usage.prompt_tokens
                token_usage['completion_tokens'] += run.usage.completion_tokens
                token_usage['total_tokens'] += run.usage.total_tokens

        return token_usage
    
    def get_message_counter(self):
        return self.counter

    def reset_message_counter(self):
        self.counter = 0
