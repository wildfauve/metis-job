from typing import Callable

from . import batch_runner


def build_batch_run(from_input: Callable,
                    transformer: Callable,
                    to_table: Callable):
    return batch_model(from_input, to_table, transformer)


def batch_model(from_input: Callable,
                to_table: Callable,
                transformer: Callable):
    return (batch_runner.Batcher().from_input(from_input)
            .with_transformer(transformer)
            .to_table(to_table))
