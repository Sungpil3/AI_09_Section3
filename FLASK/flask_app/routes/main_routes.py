from flask import Blueprint

bp = Blueprint('main', __name__, url_prefix='/')

@bp.route('/')
def index():
    return '/ 뒤에 result/유저닉네임을 입력해 주세요\nData based on NEXON DEVELOPERS'