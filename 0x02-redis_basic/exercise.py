#!/usr/bin/env python3
"""Redis and Python exercise"""

import uuid  # Importing the UUID library for generating unique keys
from functools import wraps  # Importing wraps for decorator functionality
from typing import Callable, Union  # Importing Callable and Union for type annotations

import redis  # Importing the Redis client library


def count_calls(method: Callable) -> Callable:
    """
    Decorator to count the number of times a method is called.
    It takes a single method Callable argument and returns a Callable.
    """
    key = method.__qualname__  # Get the qualified name of the method

    @wraps(method)
    def wrapper(self, *args, **kwargs):
        """
        Wrapper function that increments the count for the key every time
        the method is called and returns the value returned by the original method.
        """
        self._redis.incr(key)  # Increment the count for the method key
        return method(self, *args, **kwargs)  # Call the original method
    return wrapper  # Return the wrapper function


def call_history(method: Callable) -> Callable:
    """
    Decorator to store the history of inputs and outputs for a particular function.
    It takes a single method Callable argument and returns a Callable.
    """
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        """
        Wrapper function that saves the input and output of each function call in Redis.
        """
        input_key = method.__qualname__ + ":inputs"  # Key for storing inputs
        output_key = method.__qualname__ + ":outputs"  # Key for storing outputs

        output = method(self, *args, **kwargs)  # Call the original method

        self._redis.rpush(input_key, str(args))  # Save the inputs in Redis
        self._redis.rpush(output_key, str(output))  # Save the outputs in Redis

        return output  # Return the output of the original method

    return wrapper  # Return the wrapper function


def replay(fn: Callable):
    """
    Function to display the history of calls of a particular function.
    It takes a single Callable argument.
    """
    r = redis.Redis()  # Create a new Redis client
    f_name = fn.__qualname__  # Get the qualified name of the function
    n_calls = r.get(f_name)  # Get the number of calls from Redis
    try:
        n_calls = n_calls.decode('utf-8')  # Decode the number of calls
    except Exception:
        n_calls = 0  # Default to 0 if decoding fails
    print(f'{f_name} was called {n_calls} times:')  # Print the number of calls

    ins = r.lrange(f_name + ":inputs", 0, -1)  # Get the list of inputs from Redis
    outs = r.lrange(f_name + ":outputs", 0, -1)  # Get the list of outputs from Redis

    for i, o in zip(ins, outs):
        try:
            i = i.decode('utf-8')  # Decode each input
        except Exception:
            i = ""
        try:
            o = o.decode('utf-8')  # Decode each output
        except Exception:
            o = ""

        print(f'{f_name}(*{i}) -> {o}')  # Print the input and output


class Cache():
    """
    Cache class that interacts with Redis for storing and retrieving data.
    """

    def __init__(self) -> None:
        """Initialize the Redis client and flush the database."""
        self._redis = redis.Redis()  # Create a Redis client
        self._redis.flushdb()  # Flush the Redis database

    @count_calls  # Apply the count_calls decorator
    @call_history  # Apply the call_history decorator
    def store(self, data: Union[str, bytes, int, float]) -> str:
        """
        Store method to save data in Redis with a randomly generated key.

        Args:
            data (Union[str, bytes, int, float]): Data to be stored.

        Returns:
            str: The randomly generated key used to store the data.
        """
        key = str(uuid.uuid4())  # Generate a random key
        self._redis.set(key, data)  # Store the data in Redis
        return key  # Return the generated key

    def get(self, key: str, fn: Callable = None) -> Union[str, bytes, int, float]:
        """
        Get method to retrieve data from Redis and optionally transform it to a Python type.

        Args:
            key (str): The key used to retrieve the data.
            fn (Callable, optional): A function to transform the data.

        Returns:
            Union[str, bytes, int, float]: The retrieved data.
        """
        data = self._redis.get(key)  # Get the data from Redis
        if fn:
            return fn(data)  # Transform the data if a function is provided
        return data  # Return the raw data

    def get_str(self, key: str) -> str:
        """
        Get method to retrieve data from Redis and transform it to a string.

        Args:
            key (str): The key used to retrieve the data.

        Returns:
            str: The retrieved data as a string.
        """
        variable = self._redis.get(key)  # Get the data from Redis
        return variable.decode("UTF-8")  # Decode the data to a string

    def get_int(self, key: str) -> int:
        """
        Get method to retrieve data from Redis and transform it to an integer.

        Args:
            key (str): The key used to retrieve the data.

        Returns:
            int: The retrieved data as an integer.
        """
        variable = self._redis.get(key)  # Get the data from Redis
        try:
            variable = int(variable.decode("UTF-8"))  # Decode the data to an integer
        except Exception:
            variable = 0  # Default to 0 if decoding fails
        return variable