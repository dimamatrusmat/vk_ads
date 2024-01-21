import json
from datetime import date

import requests
import logging

logger = logging



class VKs_new:
    # clien это массив в котором лежат [[CLIENT_ID, CLIENT_SECRET], [CLIENT_ID, CLIENT_SECRET]]
    def __init__(self, clients):
        self.vks = []

        self.clients = clients
        self.data = None  # json_data

        self.date_from = None
        self.date_to = None
        self.headers = None

        self.start()

    # Запуск токенов
    def start(self):
        for client in self.clients:
            if len(client) > 1:
                vk = self.VK_new(client[0], client[1])  # Создаем ВК объекты
                self.vks.append(vk)

    # Проверка работоспособности токенов
    def validate(self):
        try:
            for vk in self.vks:
                if not vk._validate():
                    return False

            return True
        except Exception as e:
            logger.exception('Ошибка валидации данных: ' + str(e))

    # Функция сохранения data в json
    def save_data_json(self, name):

        with open(f'{name}.json', 'w', encoding='utf-8') as f:
            # Функция `dump` записывает объект data в открытый файл
            json.dump(self.data, f, ensure_ascii=False, indent=4)

    # Функция выдачи названий столбцов для нового кабинета
    def get_new_header(self, d):
        try:
            return f"{', '.join(d.keys())}"
        except Exception as e:
            logger.exception('Ошибка выдачи названий столбцов для нового кабинета ' + str(e))


    # Функция добавления родителей к баннеру
    def _refresh_banners_included(self, data_banners):
        try:
            data = {}
            for vk in self.vks:
                banners = vk._get_banners()

                for banner in banners:
                    ad_plan = vk._get_ad_plan_by_ad_group(banner['ad_group_id'])

                    data[banner['id']] = {
                        'ad_group_id': banner['ad_group_id'],
                        'ad_plan_id': ad_plan
                    }

            for ban in data_banners:
                banner_data = data.get(ban['id'])
                if banner_data:
                    ban.update(banner_data)

            return data_banners
        except Exception as e:
            logger.exception('Ошибка добавления родителей к баннерам: ' + str(e))

    # Функция выдачи статистики для нового кабинета
    def get_statistics(self, date1, date2=date.today().strftime("%Y-%m-%d"), id=None):
        try:
            self.date_from = date1
            self.date_to = date2
            statics = []
            # собираем статичтику в массив
            for vk in self.vks:
                logger.info('Работаю  с новым кабинетом ')
                stats = vk._get_statistic(date_from=self.date_from, date_to=self.date_to, id=id)

                if stats:
                    statics.append(stats)
            # объединяем все json в один
            statics = self._merge_dicts(*statics)
            data = self.get_data_from_hedears(statics) # изменяем статистику в вид 1 строки + все поглученные данные такие как base убираем и к их значениям добавляем dict в начало названия
            data = self._refresh_banners_included(data) # тут добавляем   'ad_group_id', 'ad_plan_id' в каждую строку
            header = self.get_new_header(data[0]).split(', ') # получаем названия столбцов

            data = self._dict_array_values_to_array(data) # наш массив dict переводи в массив массивов
            return header, data
        except Exception as e:
            logger.exception('Ошибка выдачи ститистики для нового кабинета: ' + str(e))

    # Функция для взятия вчех занчения из масива dict и засунуть все в 1 массив массивов
    def _dict_array_values_to_array(self, dict_array):
        values_array = []
        for dictionary in dict_array:
            values_array.append([*dictionary.values()])
        return values_array
    # Доп функция для изменения ключа в словоре data
    def _prefix_dict(self, d, prefix):
        return {f"{prefix}_{key}": value for key, value in d.items()}

    # Генерация данных в готовый файл со всеми данными в 1 строку
    def get_data_from_hedears(self, statics):
        try:
            datavks = []
            statics = statics['items']
            for d in statics:
                # убираем total
                del d['total']
                datavk = {}

                datavk['id'] = d['id']
                for v1 in d['rows']:
                    datavk['date'] = v1['date']

                    base = v1['base']

                    # Если есть такие данные то их удаляем
                    if 'vk' in base:
                        del base['vk']

                    dictionaries = {
                        "base": base,
                        "events": v1['events'],
                        "uniques": v1['uniques'],
                        "uniques_video": v1['uniques_video'],
                        "video": v1['video'],
                        "carousel": v1['carousel'],
                        "ad_offers": v1['ad_offers'],
                        "playable": v1['playable'],
                        "tps": v1['tps'],
                        "moat": v1['moat'],
                        "social_network": v1['social_network'],
                        "romi": v1['romi'],
                    }

                    for prefix, d in dictionaries.items():
                        datavk.update(self._prefix_dict(d, prefix))

                datavks.append(datavk)

            return datavks
        except Exception as e:
            logger.exception('Ошибка генерации data: ' + str(e))

    # функция объединения json dict
    def _merge_dicts(self, *dicts):
        r = {}
        for d in dicts:
            for k, v in d.items():
                r.setdefault(k, []).extend(v)
        return r

    class VK_new:

        def __init__(self, client_id, client_secret):
            self.access_token = None
            self.refresh_token = None

            self.client_id = client_id
            self.client_secret = client_secret

            self._start_token()

        # Функция проверки токена
        def _validate(self):
            try:
                res = self._use_method('/api/v1/urls', {'url': 'https://vk.com/vk:'})
                if res['id']:
                    logger.info('Валидация вк токена пройдена')
                    return True

            except Exception as e:
                logger.exception('Валидация токена вк не пройдена ' + res + str(e))

            return False

        # Функция удаления всех токенов на аккаунте
        def _delete_token(self, user_id):
            try:
                payload = {
                    'client_id': self.client_id,
                    'client_secret': self.client_secret,
                    'user_id': user_id
                }
                headers = {'Content-type': 'application/x-www-form-urlencoded'}

                res = requests.post('https://ads.vk.com/api/v2/oauth2/token/delete.json', data=payload, headers=headers)
                return res

            except Exception as e:
                logger.exception('Функцией удлаения токенов воспользоваться не удалось: ' + str(e))

                return False

        # Функция обновления токена, если токен объекта VK истек
        def _refresh_token(self):
            try:
                payload = {
                    'client_id': self.client_id,
                    'client_secret': self.client_secret,
                    'grant_type': 'refresh_token',
                    'refresh_token': self.refresh_token
                }
                headers = {'Content-type': 'application/x-www-form-urlencoded'}

                res = requests.post('https://ads.vk.com/api/v2/oauth2/token.json', data=payload, headers=headers).json()
                self.access_token = res['access_token']

                return True
            except Exception as e:
                logger.exception('Функцией обновления токена воспользоваться не удалось: ' + str(e))

                return False

        # Функция получения access_token. Запускается, когда vk проодит через инит
        def _start_token(self):
            try:
                payload = {
                    'client_id': self.client_id,
                    'client_secret': self.client_secret,
                    'grant_type': 'client_credentials',
                    'scope': ["read_ads"]
                }
                # обяхательный параметр в хедер
                headers = {'Content-type': 'application/x-www-form-urlencoded'}

                res = requests.post('https://ads.vk.com/api/v2/oauth2/token.json', data=payload, headers=headers).json()

                self.access_token = res['access_token']
                self.refresh_token = res['refresh_token']

            except Exception as e:
                logger.info('Ошибка получения вк токена: ' + str(e) + '\n Пытаюсь повторить после удаления токенов')
                try:
                    self._delete_token(res['user_id'])
                    self._start_token()
                except Exception as e:
                    logger.error(res['error'])
                    logger.exception('Ошибка получения токена: ' + str(e))

        # Функция получения компании по группе баннеров
        def _get_ad_plan_by_ad_group(self, ad_group):
            try:
                ad_plan = self._use_method(f'/api/v2/ad_groups/{ad_group}.json', {'fields': 'ad_plan_id'})

                return ad_plan['ad_plan_id']
            except Exception as e:
                logger.exception('Ошибка получения ид банеров' + str(e))

        # Функция получения всех баннеров и групп баннеров на аккаунте
        def _get_banners(self):
            try:
                banners = self._use_method(f'/api/v2/banners.json', {'fields': 'id,ad_group_id'})

                return banners['items']
            except Exception as e:
                logger.exception('Ошибка получения ид банеров' + str(e))

        # Функция получения статистики
        def _get_statistic(self, date_from, date_to, metrics='all', id=None, company='banners'):
            try:
                param = {
                    'date_from': date_from,
                    'date_to': date_to,
                    'metrics': metrics,
                }

                if id:
                    param['id'] = id

                res = self._use_method(f'/api/v2/statistics/{company}/day.json', param)
                del res['total']

                return res

            except Exception as e:
                logger.exception('ВК статистика не получена: ' + str(e) + str(res.get('error')))

        # Доп функция для использования методов api
        def _use_method(self, method, param):
            try:
                headers = {'Content-type': 'application/x-www-form-encoded',
                           "Authorization": f"Bearer {self.access_token}"}

                url = f'https://ads.vk.com{method}'

                res = requests.get(url, params=param, headers=headers)

                if 'Access token is expired' in res.text:
                    self.refresh_token()
                    self._use_method(self, method, param)
                else:
                    return res.json()
            except Exception as e:
                logger.exception(f'ВК метод {method} выдал: ' + str(e))

        # Получить лимиты в виде "Секунды": "кол-во сколько можно исполдьзовать"
        def _get_limit(self):
            try:
                res = self._use_method('/api/v2/throttling.json', {})

                v2 = res['statapid_generic']['v2']['READ']['limits']
                v3 = res['statapid_generic']['v3']['']['limits']

                return v2, v3
            except Exception as e:
                logger.exception('ВК литимы не удалось получить ' + str(e))


if __name__ == '__main__':
    # @abolior - tg
    clients = [['zawG5V96vFJ12EJI',
                '7E1GWhpLPaCcHtmtayt15C7aPJWSVLxS2X3C3SgA8BalOuYCZj9JhhE3bL6mEfuVovpj4JHK6OU74JGjk9RKsPjfhqzbuDzm1BgWMJ9kocc9gjPxA6MDChgVY4WFwfuhAtixAIqy4ZWwaRklGQJUbiGIP2w2f2syjea6Ru2gyV75LJR8kBf0riLaycaXM846s401zZhqWXJc831IYdR7AMSLzqvetRxEDMn0Du3PXo2ygUI1vAROOKu'],]
    vks = VKs_new(clients)
    print(vks.get_statistics('2023-07-24', '2023-08-26'))
