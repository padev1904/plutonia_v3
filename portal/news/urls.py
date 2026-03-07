from django.urls import path

from . import views

app_name = "news"

urlpatterns = [
    path("", views.article_list, name="article-list"),
    path("resources/", views.resource_list, name="resource-list"),
    path("resources/item/<int:pk>/", views.resource_detail, name="resource-detail"),
    path("resources/submit/", views.resource_submit, name="resource-submit"),
    path("resources/preview/<uuid:token>/", views.resource_preview, name="resource-preview"),
    path("article/<int:pk>/", views.article_detail, name="article-detail"),
    path("article/<int:pk>/card/", views.article_card_public, name="article-card-public"),
    path("preview/card/<uuid:token>/", views.article_card_preview, name="article-card-preview"),
    path("preview/<uuid:token>/", views.article_preview, name="article-preview"),
    path("article/<int:pk>/favorite/", views.toggle_favorite, name="toggle-favorite"),
    path("categories/", views.category_list, name="category-list"),
    path("category/<slug:slug>/", views.category_detail, name="category-detail"),
    path("favorites/", views.favorites, name="favorites"),
    path("dashboard/", views.dashboard, name="dashboard"),
]
