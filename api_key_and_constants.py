import os

API_KEY = os.getenv('KAIKO_API_KEY')
API_KEY = '2u8d86u372f3r2a1rme8l0463saycq4h'

if API_KEY is None:
    raise 'Missing API_KEY'
