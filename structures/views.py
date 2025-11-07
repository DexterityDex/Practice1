from flask import render_template
from config import app, db
from models import ContentType, Country, Rating, NetflixContent
from sqlalchemy import func, desc


@app.template_filter('format_seasons')
def format_seasons_filter(number):
    return format_seasons(number)

def format_seasons(number):
    if number is None:
        return "неизвестно"

    # Преобразуем в целое число для уверенности
    number = int(number)

    # Получаем последнюю цифру и последние две цифры
    last_digit = number % 10
    last_two_digits = number % 100

    # Правила склонения
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

    # Запрос 1: Топ-10 самых новых фильмов
    query1_headers = ["Название", "Год выпуска", "Рейтинг", "Длительность"]
    query1_data = db.session.query(
        NetflixContent.title,
        NetflixContent.release_year,
        Rating.name,
        db.case(
            (ContentType.name == 'Movie', db.cast(NetflixContent.duration_minutes, db.String) + ' мин.'),
            # Возвращаем только числовое значение для сезонов
            (ContentType.name == 'TV Show', db.cast(NetflixContent.duration_seasons, db.String)),
            else_='Неизвестно'
        ).label('duration'),
        ContentType.name.label('content_type')  # Добавляем тип контента для определения формата вывода
    ).join(
        Rating, NetflixContent.rating_id == Rating.identifier
    ).join(
        ContentType, NetflixContent.type_id == ContentType.identifier
    ).filter(
        db.or_(
            db.and_(ContentType.name == 'Movie', NetflixContent.duration_minutes.isnot(None)),
            db.and_(ContentType.name == 'TV Show', NetflixContent.duration_seasons.isnot(None))
        )
    ).order_by(
        NetflixContent.release_year.desc()
    ).limit(10).all()

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

    # Запрос 2: Топ-5 стран по количеству контента
    query2_headers = ["Страна", "Количество контента"]
    query2_data = db.session.query(
        Country.name,
        func.count(NetflixContent.show_id)
    ).join(
        NetflixContent, NetflixContent.country_id == Country.identifier
    ).group_by(
        Country.name
    ).order_by(
        func.count(NetflixContent.show_id).desc()
    ).limit(5).all()

    # Запрос 3: Распределение контента по рейтингам
    query3_headers = ["Рейтинг", "Количество", "Год с наибольшим количеством релизов"]

    # Формируем данные для каждого рейтинга
    query3_data = []

    # Получаем все рейтинги
    ratings = Rating.query.all()

    for rating in ratings:
        # Считаем общее количество контента для этого рейтинга
        content_count = db.session.query(func.count(NetflixContent.show_id)).filter(
            NetflixContent.rating_id == rating.identifier
        ).scalar() or 0

        # Находим год с наибольшим количеством релизов для этого рейтинга
        year_counts = db.session.query(
            NetflixContent.release_year,
            func.count(NetflixContent.show_id).label('count')
        ).filter(
            NetflixContent.rating_id == rating.identifier,
            NetflixContent.release_year.isnot(None)
        ).group_by(
            NetflixContent.release_year
        ).all()

        if not year_counts:
            best_year = 0
        else:
            # Находим год с максимальным количеством и, при равенстве, берем самый последний
            max_count = max(year_counts, key=lambda x: x[1])[1]
            best_years = [y for y, c in year_counts if c == max_count]
            best_year = max(best_years) if best_years else 0

        query3_data.append((rating.name, content_count, best_year))

    # Сортируем по убыванию количества контента
    query3_data.sort(key=lambda x: x[1], reverse=True)

    # Запрос 4: Количество добавлений по годам
    query4_headers = ["Год добавления", "Фильмов", "Сериалов", "Всего"]

    # Подзапрос для получения года из даты
    year_extract = func.strftime('%Y', NetflixContent.date_added).label('year_added')

    # Получаем статистику по фильмам
    movies_by_year = db.session.query(
        year_extract,
        func.count(NetflixContent.show_id)
    ).join(
        ContentType, NetflixContent.type_id == ContentType.identifier
    ).filter(
        ContentType.name == 'Movie',
        NetflixContent.date_added.isnot(None)
    ).group_by(
        year_extract
    ).all()

    # Получаем статистику по сериалам
    series_by_year = db.session.query(
        year_extract,
        func.count(NetflixContent.show_id)
    ).join(
        ContentType, NetflixContent.type_id == ContentType.identifier
    ).filter(
        ContentType.name == 'TV Show',
        NetflixContent.date_added.isnot(None)
    ).group_by(
        year_extract
    ).all()

    # Собираем данные вместе
    years_data = {}
    for year, count in movies_by_year:
        if year and year.strip():
            years_data[year] = {'movies': count, 'series': 0, 'total': count}

    for year, count in series_by_year:
        if year and year.strip():
            if year in years_data:
                years_data[year]['series'] = count
                years_data[year]['total'] += count
            else:
                years_data[year] = {'movies': 0, 'series': count, 'total': count}

    query4_data = [
        (year, data['movies'], data['series'], data['total'])
        for year, data in sorted(years_data.items(), key=lambda x: x[0], reverse=True)
    ]

    # Запрос 5: Топ-5 режиссеров с наибольшим количеством контента
    query5_headers = ["Режиссер", "Количество фильмов/сериалов"]

    # Разбиваем поле director на отдельных режиссеров
    directors_data = {}

    # Получаем все записи с режиссерами
    contents_with_directors = db.session.query(
        NetflixContent.director
    ).filter(
        NetflixContent.director.isnot(None),
        NetflixContent.director != ''
    ).all()

    # Подсчитываем количество фильмов у каждого режиссера
    for content in contents_with_directors:
        if content.director:
            director_list = [d.strip() for d in content.director.split(',')]
            for director in director_list:
                if director:
                    if director in directors_data:
                        directors_data[director] += 1
                    else:
                        directors_data[director] = 1

    # Сортируем и берем топ-5
    query5_data = sorted(
        [(director, count) for director, count in directors_data.items()],
        key=lambda x: x[1], reverse=True
    )[:5]

    return render_template('index.html',
                           content_types=content_types,
                           countries=countries,
                           ratings=ratings,
                           query1=[query1_headers, formatted_query1_data],  # Используем форматированные данные
                           query2=[query2_headers, query2_data],
                           query3=[query3_headers, query3_data],
                           query4=[query4_headers, query4_data],
                           query5=[query5_headers, query5_data])