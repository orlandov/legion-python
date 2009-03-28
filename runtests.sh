#!/bin/bash

./shake --master --debug &
nosetests test_shake.py
