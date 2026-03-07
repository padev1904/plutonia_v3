from django.db import migrations


def backfill_resource_text_fields(apps, _schema_editor):
    Resource = apps.get_model("news", "Resource")
    for row in Resource.objects.all().only("id", "summary", "article_body", "description"):
        summary = (row.summary or "").strip()
        article_body = (row.article_body or "").strip()
        description = (row.description or "").strip()
        updates = []
        if not summary and description:
            row.summary = description
            updates.append("summary")
        if not article_body:
            row.article_body = description or summary
            updates.append("article_body")
        if updates:
            row.save(update_fields=updates)


def noop_reverse(_apps, _schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("news", "0013_rename_news_resour_review__c626a4_idx_news_resour_review__b07e79_idx_and_more"),
    ]

    operations = [
        migrations.RunPython(backfill_resource_text_fields, noop_reverse),
    ]
