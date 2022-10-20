import os

API_KEY = os.getenv('KAIKO_API_KEY')

if API_KEY is None:
    raise 'Missing API_KEY'
