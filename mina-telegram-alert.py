#!/usr/bin/env python3
import requests, configparser
import os
import pandas as pd
import json
import base58
import time
import urllib.request
from google.cloud import bigquery

pd.options.mode.chained_assignment = None 

# Mina constants
MINA_DECIMALS = 1 / 1000000000 
SLEEP_TIME = 60

class MinaTelegram():
    def __init__( self, config_file='config.ini' ):
        # read the config and setup telegram
        self.name = os.uname()[1]
        self.read_config( config_file )
        self.setup_telegram()
        self.public_key = self.config['Mina']['public_key']
        self.client = bigquery.Client()
        
        # set the bigquery variable
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.config['BigQuery']['credentials'] 

        # get the provider info
        self.providers = self.get_providers( )

        # hello message 
        self.send( f'{self.name}: Hello from Mina Watcher!' )
        
        while True:
            # obtain blocks
            blocks = self.get_blocks( self.config['Mina']['last_block'] )

            # check if the block is empty
            if blocks.empty:
                print( f'Empty Blocks - Sleeping for {SLEEP_TIME}')
                time.sleep( SLEEP_TIME )
                continue

            block_list = blocks['blockheight'].unique()
            block_list.sort()

            # save the datetime
            blocks['datetime'] = blocks['datetime'].apply(lambda x : pd.to_datetime(str(x)))
            blocks['receivedtime'] = blocks['receivedtime'].apply(lambda x : pd.to_datetime(str(x)))
            blocks['date'] = blocks['datetime'].dt.date
            blocks['time'] = blocks['datetime'].dt.time
            blocks['received_date'] = blocks['receivedtime'].dt.date
            blocks['received_time'] = blocks['receivedtime'].dt.time
            blocks['delta_time'] = blocks['receivedtime'] - blocks['datetime']
            blocks['delta_time'] = blocks['delta_time'].apply(lambda x : x.total_seconds())

            # parse the blocks
            for blockheight in block_list:
                print( f'Parsing blockheight: {blockheight}')
                # obtain all the blocks of the block height
                blocks_of_height = blocks.loc[blocks['blockheight'] == blockheight] 
                self.parse_blocks( blocks_of_height )

            # save / update the config file
            self.config['Mina']['last_block'] = str( blocks['blockheight'].max() )
            with open( config_file, 'w') as configfile:
                self.config.write(configfile)
            
            print( f'Sleeping for {SLEEP_TIME}')
            time.sleep( SLEEP_TIME )
        
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
        self.telegram_token = self.config['Telegram']['telegram_token']
        self.telegram_chat_id = self.config['Telegram']['telegram_chat_id']

    def send( self, msg ):
        '''
        Send telegram message
        '''
        requests.post( f'https://api.telegram.org/bot{self.telegram_token}/sendMessage?chat_id={self.telegram_chat_id}&text={msg}' )
        print( msg )

    def get_blocks( self, target_blockheight ):
        '''Get the blocks'''
        query = """
        SELECT blockheight,
            creator,
            canonical,
            datetime,
            receivedtime,
            transactions,
            statehash,
        FROM minaexplorer.archive.blocks
        WHERE blockheight > %s
        ORDER BY blockheight DESC""" % target_blockheight
        query_job = self.client.query(query)
        iterator = query_job.result()
        rows = list(iterator)

        # if the query returns no data, return empty dataframe
        if len( rows ) == 0:
            return pd.DataFrame()
        # Transform the rows into a nice pandas dataframe
        df = pd.DataFrame(data=[list(x.values()) for x in rows], columns=list(rows[0].keys()))
        df.drop_duplicates(subset=['statehash'])
        df.sort_values(by=['blockheight'], inplace=True)
        return df

    def get_providers( self ):
        '''get provider list'''
        output = dict()
        # staketab providers
        with urllib.request.urlopen(self.config['Providers']['staketab']) as url:
            data = json.loads(url.read().decode())
        for provider in data['staking_providers']:
            output[ provider['provider_address'] ] = provider['provider_title'] 

        # Mina Foundation
        mf_data = self.get_csv( self.config['Providers']['mina_foundation'] )
        for idx, address in enumerate(mf_data):
            output[ address ] = f'Mina Foundation {idx}'

        # O1 Labs
        mf_data = self.get_csv( self.config['Providers']['o1_labs'] )
        for idx, address in enumerate(mf_data):
            output[ address ] = f'O1 Labs {idx}'

        return output

    def get_csv( self, url ):
        '''return the csv as a list'''
        req = urllib.request.Request( url )
        req.add_header('User-Agent', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:77.0) Gecko/20100101 Firefox/77.0')
        content = urllib.request.urlopen(req)
        data = pd.read_csv(content, header=None)
        return list( data[0] ) 

    def get_provider( self, address ):
        '''return the provider name if it is in the provider dictionary'''
        if address in self.providers.keys():
            address = self.providers[ address ]
        return address

    def parse_blocks( self, blocks ):
        '''parse the blocks of the same blockheight'''
        # parse the canonical
        blocks.sort_values(by=['canonical'], inplace=True, ascending=False)
        for index, block in blocks.iterrows():
            self.parse_block( block )

    def parse_block( self, block ):
        '''parse the block'''
        # canonical flag
        canonical = 'Canonical' if block.canonical == True else 'Non-Canonical'
        # parse the transactions
        transactions = self.parse_transactions( block.transactions )

        # check if the creator of the block is the public_key
        if block.creator == self.public_key:
            self.send( f"{canonical} {block.blockheight}: Created Block - {self.get_provider( transactions['coinbase_receiver'] )} Received {transactions['coinbase_reward']} at { block.date } { block.time } [ { block.delta_time } ]" )

        # check the transactions
        if canonical == 'Canonical':
            for transaction in transactions['user_commands']:
                if self.public_key in [transaction['from'], transaction['to']]:
                    # if it is a delegation, omit the amount
                    if transaction['kind'] == 'STAKE_DELEGATION':
                        self.send( f"{canonical} {block.blockheight}: Stake Delegation from {self.get_provider( transaction['from'] )} to {self.get_provider( transaction['to'])} [{transaction['memo'].strip()}] at { block.date } { block.time }" )
                    else:
                        self.send( f"{canonical} {block.blockheight}: {transaction['kind'].capitalize()} from {self.get_provider( transaction['from'] )} to {self.get_provider( transaction['to'])} for {transaction['amount']} [{transaction['memo'].strip()}] at { block.date } { block.time }" )


    def parse_transactions( self, transactions ):
        '''parse transactions'''
        if transactions['feetransfer'] == None or transactions['usercommands'] == None:
            return { 'coinbase_reward' : 0,
                     'coinbase_receiver' : '',
                     'user_commands' : [] }
        
        output = {}
        # parse the rewards
        output['coinbase_reward'] = transactions['coinbase'] * MINA_DECIMALS
        output['coinbase_receiver']= transactions['coinbasereceiveraccount']['publickey']
        output['fee'] = []

        # parse the fee transfers
        for fee_transfer in json.loads( transactions['feetransfer'] ):
            output['fee'].append( fee_transfer['fee'] * MINA_DECIMALS )

        # user commands
        output['user_commands'] = []
        for user_command in json.loads( transactions['usercommands'] ):
            user_tx = { 'from' : user_command['from'],
                        'to' :   user_command['to'],
                        'amount': round( user_command['amount'] * MINA_DECIMALS, 4 ),
                        'fee' : round( user_command['fee'] * MINA_DECIMALS, 4),
                        'kind' : user_command['kind'],
                        'memo': self.decode_memo( user_command['memo'] ) }
            output['user_commands'].append( user_tx )
        return output

    def decode_memo( self, memo ):
        '''decode the memo'''
        decoded = base58.b58decode( memo )[2:-4]
        return decoded.decode("utf-8", "strict")

mina_bot = MinaTelegram()

