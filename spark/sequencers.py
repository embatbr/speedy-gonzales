# -*- coding: utf-8 -*-


import functions


class SequenceExecutor(object):

    def __init__(self, memory, block_sequence):
        self.memory = memory
        self.block_sequence = block_sequence

    def execute(self):
        for block in self.block_sequence:
            function = block['function']
            args = block['args']

            function(self.memory, *args)


class SequenceBuilder(object):

    def __init__(self, spark_context):
        self.spark_context = spark_context

    def build(self, steps):
        memory = {
            'spark_context': self.spark_context
        }

        block_sequence = list()

        for step in steps:
            block_sequence.append({
                'function': getattr(functions, step[0]),
                'args': step[1:]
            })

        return SequenceExecutor(memory, block_sequence)
