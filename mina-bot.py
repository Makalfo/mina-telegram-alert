#!/usr/bin/env python3

import requests, configparser
import os

class MinaTelegram():
    def __init__( self, config_file='config.ini' ):
        # read the config and setup telegram
        self.name = os.uname()[1]
        self.read_config( config_file )
        self.setup_telegram()
        self.public_key = self.config['Mina']['publicKey']
        self.graphql_api = self.config['GraphQL']['api']
        
        # hello message 
        self.send( f'{self.name}: Hello from Mina Transaction Watcher!' )
        response = self.get_transactions( { 'public_key': self.public_key,
                                            'limit': 10 } )    
        for tx in response:
            print( tx )
        
    def read_config( self, config_file ):
        '''
        Read the configuration file
        '''
        config = configparser.ConfigParser()
        config.read( config_file )
        self.config = config

    def setup_telegram( self ):
        '''
        Setup telegram
        '''
        self.telegramToken = self.config['Telegram']['telegramToken']
        self.telegramChatId = self.config['Telegram']['telegramChatID']

    def send( self, msg ):
        '''
        Send telegram message
        '''
        requests.post( f'https://api.telegram.org/bot{self.telegramToken}/sendMessage?chat_id={self.telegramChatId}&text={msg}' )

    def _graphql_request(self, query: str, variables: dict = {}):
        '''
        GraphQL queries all look alike, this is a generic function to facilitate a GraphQL Request.
        Arguments: query {str} -- A GraphQL Query
        Keyword Arguments: variables {dict} -- Optional Variables for the GraphQL Query (default: {{}})
        Raises: Exception: Raises an exception if the response is anything other than 200.
        Returns: dict -- Returns the JSON Response as a Dict.
        '''
        # Strip all the whitespace and replace with spaces
        query = " ".join(query.split())
        payload = {'query': query}
        if variables:
            payload = {**payload, 'variables': variables}

        headers = {"Accept": "application/json"}
        response = requests.post(self.graphql_api,
                                json = payload,
                                headers = headers)
        print( payload )
        resp_json = response.json()
        if response.status_code == 200 and "errors" not in resp_json:
            return resp_json
        else:
            print(response.text)
            raise Exception("Query failed -- returned code {}. {}".format(
                response.status_code, query))

    def get_transactions( self, variables ):
        '''
        obtain the last transactions
        '''
        query = '''query($public_key: String!, $limit: Int){
                transactions(limit: $limit, sortBy: DATETIME_DESC, query: {
                    canonical: true,
                    OR: [{
                    to: $public_key
                    }, {
                    from: $public_key
                    }]
                }) {
                    fee
                    from
                    to
                    nonce
                    amount
                    memo
                    hash
                    kind
                    dateTime
                    block {
                    blockHeight
                    stateHash
                    }
                }
            }
        '''
        print( query )

        return self._graphql_request(query, variables)['data']['transactions']

mina_bot = MinaTelegram()

