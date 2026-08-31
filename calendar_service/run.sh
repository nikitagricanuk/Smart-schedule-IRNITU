#!/bin/bash

exec gunicorn --bind=0.0.0.0:80 --workers=2 wsgi:app
