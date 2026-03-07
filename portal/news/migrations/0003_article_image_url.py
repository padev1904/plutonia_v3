from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("news", "0002_setup_search_vector_trigger"),
    ]

    operations = [
        migrations.AddField(
            model_name="article",
            name="image_url",
            field=models.URLField(blank=True, default="", max_length=2000),
        ),
    ]
