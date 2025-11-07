import csv
from datetime import datetime
from config import app, db
from models import ContentType, Country, Rating, NetflixContent


def upload_data_from_csv(path):
    # Очистка базы данных перед загрузкой
    db.session.query(NetflixContent).delete()
    db.session.query(ContentType).delete()
    db.session.query(Country).delete()
    db.session.query(Rating).delete()
    db.session.commit()

    content_types = {}
    countries = {}
    ratings = {}

    with open(path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            # Обработка типа контента
            type_name = row['type'].strip() if row['type'] else 'Неизвестно'
            if type_name not in content_types:
                content_type = ContentType(type_name)
                db.session.add(content_type)
                db.session.commit()
                content_types[type_name] = content_type

            # Обработка страны
            country_name = row['country'].strip() if row['country'] else None
            country_id = None
            if country_name:
                if country_name not in countries:
                    country = Country(country_name)
                    db.session.add(country)
                    db.session.commit()
                    countries[country_name] = country
                country_id = countries[country_name].identifier

            # Обработка рейтинга
            rating_name = row['rating'].strip() if row['rating'] else 'Не указан'
            if rating_name not in ratings:
                rating = Rating(rating_name)
                db.session.add(rating)
                db.session.commit()
                ratings[rating_name] = rating

            # Обработка даты добавления
            date_added = None
            if row['date_added'] and row['date_added'].strip():
                try:
                    date_added = datetime.strptime(row['date_added'].strip(), '%B %d, %Y').date()
                except ValueError:
                    try:
                        date_added = datetime.strptime(row['date_added'].strip(), '%Y-%m-%d').date()
                    except ValueError:
                        print(f"Не удалось обработать дату: {row['date_added']}")

            # Обработка года выпуска
            release_year = None
            if row['release_year'] and row['release_year'].strip():
                try:
                    release_year = int(row['release_year'])
                except ValueError:
                    print(f"Не удалось обработать год: {row['release_year']}")

                    # Обработка длительности
                    duration_minutes = None
                    duration_seasons = None

                    if row['duration'] and row['duration'].strip():
                        duration = row['duration'].strip()
                        if 'min' in duration:
                            try:
                                duration_minutes = int(duration.replace('min', '').strip())
                            except ValueError:
                                print(f"Не удалось обработать длительность в минутах: {duration}")
                        elif 'Season' in duration or 'Seasons' in duration:
                            try:
                                duration_seasons = int(duration.replace('Seasons', '').replace('Season', '').strip())
                            except ValueError:
                                print(f"Не удалось обработать количество сезонов: {duration}")

            # Добавление записи о контенте
            content = NetflixContent(
                show_id=row['show_id'].strip(),
                title=row['title'].strip(),
                type_id=content_types[type_name].identifier,
                director=row['director'].strip() if row['director'] else None,
                cast=row['cast'].strip() if row['cast'] else None,
                country_id=country_id,
                date_added=date_added,
                release_year=release_year,
                rating_id=ratings[rating_name].identifier,
                duration_minutes=duration_minutes,
                duration_seasons=duration_seasons
            )
            db.session.add(content)
            print(f"Добавлен контент: {row['title']}")

        db.session.commit()
        print("Данные успешно загружены в базу данных!")


# Загрузка данных в таблицы
upload_data_from_csv("data/netflix_titles.csv")