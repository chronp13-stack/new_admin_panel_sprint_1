from django.contrib import admin
from .models import Genre, FilmWork, GenreFilmWork, Person, PersonFilmWork


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    # Отображение полей в списке
    list_display = ("name", "description", "created", "modified")

    # Поиск по полям
    search_fields = ("name", "description", "id")
    pass


class GenreFilmWorkInline(admin.TabularInline):
    model = GenreFilmWork


class PersonFilmWorkInline(admin.TabularInline):
    model = PersonFilmWork


@admin.register(FilmWork)
class FilmWorkAdmin(admin.ModelAdmin):
    inlines = (GenreFilmWorkInline, PersonFilmWorkInline)
    # Отображение полей в списке
    list_display = ("title", "type", "creation_date", "rating", "created", "modified")

    # Фильтрация в списке
    list_filter = (
        "type",
        "rating",
    )

    # Поиск по полям
    search_fields = ("title", "description", "id")


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    # Отображение полей в списке
    list_display = ("full_name", "created", "modified")

    # Поиск по полям
    search_fields = ("full_name", "id")

    pass
