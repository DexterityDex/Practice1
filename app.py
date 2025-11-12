# from flask import Flask
#
# app = Flask(__name__)
#
# if __name__ == '__main__':
#     app.run(debug=True)
#
# from structures.views import index

from config import app

from structures.views import *

if __name__ == '__main__':
    app.run(debug=True)