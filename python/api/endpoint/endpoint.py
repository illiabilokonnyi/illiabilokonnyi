from __future__ import annotations
import asyncio
import logging
from quart import Quart, request, jsonify


app = Quart(__name__)


async def _parse_copy_params() -> (bytes, bool, str, str):
    try:
        data = await request.data
        token_prod = request.headers.get('token_prod', '')
        token_dev = request.headers.get('token_dev', '')
        approve = request.args.get('approve', default=False, type=lambda v: v.lower().lstrip().rstrip() == 'true')

        _validate_copy_params(data, token_prod, token_dev)

        return data, approve, token_prod, token_dev

    except BaseException as exc:
        logging.debug(str(exc))
        raise exc


def _validate_copy_params(data, token_prod, token_dev) -> None:
    if not data:
        raise ValueError('A config file with at least one record must be provided with the request')
    if not any([token_dev,
                token_prod]):
        raise ValueError('At least one token must be provided with the request')


async def _copy(data: bytes,
                approve: bool,
                token_prod: str,
                token_dev: str):
    try:
        # running async logic...
        # ...
        response = {'status': 'IN_PROGRESS'}
        return jsonify(response)
    except BaseException as exc:
        return jsonify({'errors': str(exc)})


@app.route("/bulk_copy", methods=['POST'])
async def bulk_copy():
    try:
        # reading request parameters
        try:
            data, approve, token_prod, token_dev = await _parse_copy_params()
        except ValueError as exc:
            return jsonify({'errors': str(exc)})

        # executing request
        return await _copy(data,
                           approve=approve,
                           token_prod=token_prod,
                           token_dev=token_dev)

    except BaseException as e:
        logging.error(str(e))


@app.route('/actuator/health')
def health_check():
    return jsonify({'status': 'UP'})


def run_app() -> None:

    from hypercorn.config import Config
    from hypercorn.asyncio import serve

    try:
        asyncio.run(serve(app, Config()))
    except BaseException as exc:
        logging.critical(str(exc))
        raise exc
