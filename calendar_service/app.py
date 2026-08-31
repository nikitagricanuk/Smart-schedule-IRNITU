from flask import Flask, Response, abort, jsonify

import ical_builder
from tools.logger import logger
from tools.storage import MongodbService

app = Flask(__name__)
storage = MongodbService().get_instance()


def _ics_response(ics_bytes: bytes, filename: str) -> Response:
    return Response(
        ics_bytes,
        mimetype='text/calendar; charset=utf-8',
        headers={'Content-Disposition': f'inline; filename="{filename}.ics"'},
    )


@app.route('/health')
def health():
    return jsonify({'status': 'ok'})


@app.route('/ical/group/<group_name>')
@app.route('/ical/group/<group_name>.ics')
def group_calendar(group_name):
    schedule_doc = storage.get_schedule(group_name=group_name)
    if not schedule_doc or not schedule_doc.get('schedule'):
        logger.warning(f'Calendar requested for unknown or empty group="{group_name}"')
        abort(404)

    ics = ical_builder.build_calendar(schedule_doc, calendar_name=f'Расписание {group_name}')
    return _ics_response(ics, group_name)


@app.route('/ical/prep/<prep_name>')
@app.route('/ical/prep/<prep_name>.ics')
def prep_calendar(prep_name):
    schedule_doc = storage.get_schedule_prep(prep_name=prep_name)
    if not schedule_doc or not schedule_doc.get('schedule'):
        logger.warning(f'Calendar requested for unknown or empty prep="{prep_name}"')
        abort(404)

    ics = ical_builder.build_calendar(schedule_doc, calendar_name=f'Расписание {prep_name}')
    return _ics_response(ics, prep_name)


if __name__ == '__main__':
    app.run(debug=True)
