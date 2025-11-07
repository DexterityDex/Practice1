from config import app, db


class ContentType(db.Model):
    __tablename__ = 'Тип'
    identifier = db.Column('ID', db.Integer, primary_key=True)
    name = db.Column('Название', db.String(50), nullable=False)
    contents = db.relationship('NetflixContent', back_populates="type_rel", cascade="all, delete")

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return f'ID: {self.identifier}, Название: {self.name}\n'


class Country(db.Model):
    __tablename__ = 'Страна'
    identifier = db.Column('ID', db.Integer, primary_key=True)
    name = db.Column('Название', db.String(100), nullable=False)
    contents = db.relationship('NetflixContent', back_populates="country_rel", cascade="all, delete")

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return f'ID: {self.identifier}, Название: {self.name}\n'


class Rating(db.Model):
    __tablename__ = 'Рейтинг'
    identifier = db.Column('ID', db.Integer, primary_key=True)
    name = db.Column('Название', db.String(50), nullable=False)
    contents = db.relationship('NetflixContent', back_populates="rating_rel", cascade="all, delete")

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return f'ID: {self.identifier}, Название: {self.name}\n'


class NetflixContent(db.Model):
    __tablename__ = 'Контент'
    show_id = db.Column('ID', db.String(20), primary_key=True)
    title = db.Column('Название', db.String(200), nullable=False)
    type_id = db.Column('Тип', db.Integer, db.ForeignKey('Тип.ID'))
    director = db.Column('Режиссер', db.Text, nullable=True)
    cast = db.Column('Актеры', db.Text, nullable=True)
    country_id = db.Column('Страна', db.Integer, db.ForeignKey('Страна.ID'), nullable=True)
    date_added = db.Column('Дата добавления', db.Date, nullable=True)
    release_year = db.Column('Год выпуска', db.Integer, nullable=True)
    rating_id = db.Column('Рейтинг', db.Integer, db.ForeignKey('Рейтинг.ID'), nullable=True)
    duration_minutes = db.Column('Длительность (минуты)', db.Integer, nullable=True)
    duration_seasons = db.Column('Количество сезонов', db.Integer, nullable=True)

    type_rel = db.relationship("ContentType", back_populates="contents")
    country_rel = db.relationship("Country", back_populates="contents")
    rating_rel = db.relationship("Rating", back_populates="contents")

    def __init__(self, show_id, title, type_id, director, cast, country_id,
                 date_added, release_year, rating_id, duration_minutes, duration_seasons):
        self.show_id = show_id
        self.title = title
        self.type_id = type_id
        self.director = director
        self.cast = cast
        self.country_id = country_id
        self.date_added = date_added
        self.release_year = release_year
        self.rating_id = rating_id
        self.duration_minutes = duration_minutes
        self.duration_seasons = duration_seasons

    def __repr__(self):
        duration_str = f"{self.duration_minutes} мин." if self.duration_minutes else f"{self.duration_seasons} сезон(ов)"
        return f'ID: {self.show_id}, Название: {self.title}, Тип: {self.type_id}, Год: {self.release_year}, Длительность: {duration_str}\n'


# Создание базы данных
app.app_context().push()

with app.app_context():
    db.create_all()