from flask_sqlalchemy import SQLAlchemy
from app import app

# Создаем расширение для работы с базой данных
db = SQLAlchemy()

# Конфигурация базы данных SQLite
app.config["SQLALCHEMY_DATABASE_URI"]        = "sqlite:///dataset.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

# Инициализация приложения с расширением
db.init_app(app)