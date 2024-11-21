from flask import Blueprint, render_template, redirect, session, url_for, send_file, request, jsonify
from db.db_service import DatabaseService
import pandas as pd
import numpy as np

main_bp = Blueprint('main', __name__, template_folder='templates',
                    static_folder='static', static_url_path='/main/static')
db_service = DatabaseService()


@main_bp.route('/')
def redirection():
    return redirect(url_for('main.page_main'))


@main_bp.route('/bonne_adresse/')
def page_main():
    return render_template('main.html')



