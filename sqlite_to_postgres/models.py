from dataclasses import dataclass
from typing import Optional


@dataclass
class Genre:
    id: str
    name: str
    description: Optional[str]
    created: str
    modified: str


@dataclass
class FilmWork:
    id: str
    title: str
    description: Optional[str]
    creation_date: Optional[str]
    rating: Optional[float]
    type: str
    created: str
    modified: str


@dataclass
class Person:
    id: str
    full_name: str
    created: str
    modified: str


@dataclass
class GenreFilmWork:
    id: str
    genre_id: str
    film_work_id: str
    created: str


@dataclass
class PersonFilmWork:
    id: str
    person_id: str
    film_work_id: str
    role: str
    created: str