#!/bin/bash

./legion --master --debug &
nosetests test_legion.py
