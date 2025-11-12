from flask import render_template
from config import app, db
from models import ContentType, Country, Rating, NetflixContent
from sqlalchemy import func, desc, Integer


@app.template_filter('format_seasons')
def format_seasons_filter(number):
    return format_seasons(number)

def format_seasons(number):
    if number is None:
        return "неизвестно"

    number = int(number)

    last_digit = number % 10
    last_two_digits = number % 100

    if last_two_digits in range(11, 15):
        return f"{number} сезонов"
    elif last_digit == 1:
        return f"{number} сезон"
    elif last_digit in [2, 3, 4]:
        return f"{number} сезона"
    else:
        return f"{number} сезонов"

@app.route('/')
def index():
    # Получаем список всех типов контента, стран и рейтингов
    content_types = ContentType.query.all()
    countries = Country.query.all()
    ratings = Rating.query.all()

    # Запрос 1: Сериалы за последний год
    # Максимальный год выпуска
    max_year = db.session.query(
        func.max(NetflixContent.release_year)
    ).filter(
        NetflixContent.release_year.isnot(None)
    ).scalar()

    query1_headers = ["Название", "Год выпуска", "Рейтинг", "Длительность"]
    query1_data = db.session.query(
        NetflixContent.title,
        NetflixContent.release_year,
        Rating.name,
        db.case(
            (ContentType.name == 'TV Show', db.cast(NetflixContent.duration_seasons, db.String)),
            else_='Неизвестно'
        ).label('duration'),
        ContentType.name.label('content_type')
    ).join(
        Rating, NetflixContent.rating_id == Rating.identifier
    ).join(
        ContentType, NetflixContent.type_id == ContentType.identifier
    ).filter(
        NetflixContent.release_year == max_year,
        db.or_(
            db.and_(ContentType.name == 'TV Show', NetflixContent.duration_seasons.isnot(None))
        )
    ).order_by(
        NetflixContent.title.asc()
    ).all()

    formatted_query1_data = []
    for title, year, rating, duration, content_type in query1_data:
        if content_type == 'TV Show' and duration != 'Неизвестно':
            try:
                formatted_duration = format_seasons(int(duration))
            except (ValueError, TypeError):
                formatted_duration = duration
        else:
            formatted_duration = duration

        formatted_query1_data.append((title, year, rating, formatted_duration))

    # Запрос 2: Страны-лидеры по количеству контента
    query2_headers = ["Страна", "Количество контента"]
    query2_data = db.session.query(
        Country.name,
        func.count(NetflixContent.show_id)
    ).join(
        NetflixContent, NetflixContent.country_id == Country.identifier
    ).group_by(
        Country.name
    ).having(
        func.count(NetflixContent.show_id) > 100
    ).order_by(
        func.count(NetflixContent.show_id).desc()
    ).all()

    # Запрос 3: Топ 1% фильмов с самой большой продолжительностью
    query3_headers = ["Название", "Год", "Длительность (мин)"]

    query3_data = db.session.query(
        NetflixContent.title,
        NetflixContent.release_year,
        NetflixContent.duration_minutes
    ).join(
        ContentType, NetflixContent.type_id == ContentType.identifier
    ).filter(
        ContentType.name == 'Movie',
        NetflixContent.duration_minutes.isnot(None)
    ).order_by(
        NetflixContent.duration_minutes.desc()
    ).limit(
        db.session.query(
            func.cast(func.count(NetflixContent.show_id) * 0.01, Integer)
        ).join(
            ContentType, NetflixContent.type_id == ContentType.identifier
        ).filter(
            ContentType.name == 'Movie',
            NetflixContent.duration_minutes.isnot(None)
        ).scalar_subquery()
    ).all()

    # Запрос 4: Количество добавлений по годам
    query4_headers = ["Год добавления", "Фильмов", "Сериалов", "Всего"]

    # Извлекаем год из даты добавления
    year_extract = func.strftime('%Y', NetflixContent.date_added).label('year_added')

    # Подзапрос для фильмов по годам
    movies_subquery = db.session.query(
        year_extract.label('year'),
        func.count(NetflixContent.show_id).label('movie_count')
    ).join(
        ContentType, NetflixContent.type_id == ContentType.identifier
    ).filter(
        ContentType.name == 'Movie',
        NetflixContent.date_added.isnot(None)
    ).group_by(
        year_extract
    ).subquery()

    # Подзапрос для сериалов по годам
    series_subquery = db.session.query(
        year_extract.label('year'),
        func.count(NetflixContent.show_id).label('series_count')
    ).join(
        ContentType, NetflixContent.type_id == ContentType.identifier
    ).filter(
        ContentType.name == 'TV Show',
        NetflixContent.date_added.isnot(None)
    ).group_by(
        year_extract
    ).subquery()

    # Подзапрос для общего количества контента по годам
    total_subquery = db.session.query(
        year_extract.label('year'),
        func.count(NetflixContent.show_id).label('total_count')
    ).filter(
        NetflixContent.date_added.isnot(None)
    ).group_by(
        year_extract
    ).subquery()

    # Объединяем результаты с помощью full outer join через UNION
    years_list = db.session.query(
        year_extract.label('year')
    ).filter(
        NetflixContent.date_added.isnot(None)
    ).group_by(
        year_extract
    ).order_by(
        year_extract.desc()
    ).subquery()

    # Финальный запрос, объединяющий все подзапросы
    query4_data = db.session.query(
        years_list.c.year,
        func.coalesce(movies_subquery.c.movie_count, 0).label('movie_count'),
        func.coalesce(series_subquery.c.series_count, 0).label('series_count'),
        func.coalesce(total_subquery.c.total_count, 0).label('total_count')
    ).outerjoin(
        movies_subquery, years_list.c.year == movies_subquery.c.year
    ).outerjoin(
        series_subquery, years_list.c.year == series_subquery.c.year
    ).outerjoin(
        total_subquery, years_list.c.year == total_subquery.c.year
    ).order_by(
        years_list.c.year.desc()
    ).all()

    # Запрос 5: Средняя длительность фильмов по годам выпуска
    query5_headers = ["Год выпуска", "Средняя длительность (мин)", "Количество фильмов"]
    query5_data = db.session.query(
        NetflixContent.release_year,
        func.avg(NetflixContent.duration_minutes).label('avg_duration'),
        func.count(NetflixContent.show_id).label('movie_count')
    ).join(
        ContentType, NetflixContent.type_id == ContentType.identifier
    ).filter(
        ContentType.name == 'Movie',
        NetflixContent.duration_minutes.isnot(None),
        NetflixContent.release_year.isnot(None)
    ).group_by(
        NetflixContent.release_year
    ).order_by(
        NetflixContent.release_year.desc()
    )

    return render_template('index.html',
                           content_types=content_types,
                           countries=countries,
                           ratings=ratings,
                           query1=[query1_headers, formatted_query1_data],
                           query2=[query2_headers, query2_data],
                           query3=[query3_headers, query3_data],
                           query4=[query4_headers, query4_data],
                           query5=[query5_headers, query5_data])