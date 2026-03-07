from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("news", "0003_article_image_url"),
    ]

    operations = [
        migrations.AddField(
            model_name="article",
            name="section",
            field=models.CharField(blank=True, default="", max_length=120),
        ),
        migrations.AddField(
            model_name="article",
            name="category",
            field=models.CharField(blank=True, default="", max_length=120),
        ),
        migrations.AddField(
            model_name="article",
            name="subcategory",
            field=models.CharField(blank=True, default="", max_length=120),
        ),
    ]
