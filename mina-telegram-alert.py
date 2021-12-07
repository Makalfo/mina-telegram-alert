#!/usr/bin/env python3
import requests, configparser
import os
import pandas as pd
import json
import base58
import time
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
        
        # hello message 
        self.send( f'{self.name}: Hello from Mina Watcher!' )
        
        while True:
            # obtain blocks
            blocks = self.get_blocks( self.config['Mina']['last_block'] )    
            block_list = list( set( [int(i) for i in blocks['blockheight'].values[:]] ) )
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
                # obtain all the blocks of the block height
                blocks_of_height = blocks.loc[blocks['blockheight'] == blockheight] 
                self.parse_blocks( blocks_of_height )

            # save / update the config file
            self.config['Mina']['last_block'] = str( blocks['blockheight'].max() )
            with open( config_file, 'w') as configfile:
                self.config.write(configfile)
            
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

        # Transform the rows into a nice pandas dataframe
        df = pd.DataFrame(data=[list(x.values()) for x in rows], columns=list(rows[0].keys()))
        df.drop_duplicates(subset=['statehash'])
        df.sort_values(by=['blockheight'], inplace=True)
        return df

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
            self.send( f"{canonical} {block.blockheight}: {transactions['coinbase_receiver']} Received {transactions['coinbase_reward']} at { block.date } { block.time } [ { block.delta_time } ]" )

        # check the transactions
        if canonical == 'Canonical':
            for transaction in transactions['user_commands']:
                if self.public_key in [transaction['from'], transaction['to']]:
                    self.send( f"{canonical} {block.blockheight}: {transaction['kind'].capitalize()} from {transaction['from']} to {transaction['to']} for {transaction['amount']} [{transaction['memo'].strip()}] at { block.date } { block.time } [ { block.delta_time } ]" )


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

