import json
import time

import requests
from time import sleep
import logging

logger = logging


# функция объединения json dict
def merge_dicts(*dicts):
    r = {}
    for d in dicts:
        for k, v in d.items():
            r.setdefault(k, []).extend(v)
    return r

class VKs:
    # clien это массив в котором лежат [[CLIENT_ID, CLIENT_SECRET], [CLIENT_ID, CLIENT_SECRET]]
    def __init__(self, clients):
        self.vks = []

        self.clients = clients
        self.statics = None  # json_header
        self.data = None  # json_data

        self.date_from = None
        self.date_to = None

        self.start()

    # Запуск токенов
    def start(self):
        for client in self.clients:
            if len(client) > 1:
                vk = self.VK(client[0], client[1])  # Создаем ВК объекты
                self.vks.append(vk)
            else:
                vk = self.Vk_old(client[0])
                print(vk._get_statistic('2022-07-05', '2024-01-08'))



    # Проверка работоспособности токенов
    def validate(self):
        for vk in self.vks:
            vk._validate()

    # Функция сохранения data в json
    def save_data_json(self, name):

        with open(f'{name}.json', 'w', encoding='utf-8') as f:
            # Функция `dump` записывает объект data в открытый файл
            json.dump(self.data, f, ensure_ascii=False, indent=4)

    # # Функция выдачи статистики по людям
    # def get_statistics_users(self, date1, date2, ids=None):
    #     return self._get_statistics(date1, date2, 'users', ids)
    #
    # # Функция выдачи статистики по рекламным компаниями
    # def get_statistics_ad_company(self, date1, date2, ids=None):
    #     return self._get_statistics(date1, date2, 'ad_plans', ids)
    #
    # # Функция выдачи статистики по группам банеров
    # def get_statistics_group_banners(self, date1, date2, ids=None):
    #     return self._get_statistics(date1, date2, 'ad_groups', ids)

    # Функция выдачи статистики по банерам
    def get_statistics_banners(self, date1, date2, ids=None):
        data_banners = self._get_statistics(date1, date2, 'banners', ids)

        return self._refresh_banners_included(data_banners)

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

            for banner in data_banners:
                banner_data = data.get(banner['id'])
                if banner_data:
                    banner.update(banner_data)

            return banner
        except Exception as e:
            logger.exception('Ошибка добавления родителей к баннерам: ' + str(e))

    # Функция выдачи статистики
    def _get_statistics(self, date1, date2, company='ad_plans', id=None):
        try:
            self.date_from = date1
            self.date_to = date2
            statics = []
            # собираем статичтику в массив
            for vk in self.vks:
                statics.append(
                    vk._get_statistic(date_from=self.date_from, date_to=self.date_to, company=company, id=id))
            # объединяем все json в один
            self.statics = merge_dicts(*statics)
            self.data = self.get_data_from_hedears(self.statics)

            return self.data
        except Exception as e:
            logger.exception('Ошибка выдачи ститистики: ' + str(e))

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

    # старый рекламный кабинет вк
    class Vk_old:

        def __init__(self, access_token):
            self.access_token = access_token
            self.v = 5.81
            self.statistics = None

        # функция валидации
        def _validate(self):
            try:
                time.sleep(0.1)
                if 'error' in str(self._get_accounts()):
                    return False

                return True
            except Exception as e:
                logger.exception('Ошибка валидации: ' + str(e))

            return False

        # Возвращает статистику показателей эффективности
        def _get_statistic(self, date1, date2, ids=None, ids_type='office', period='day'):
            try:
                account_id = ids
                if not ids:
                    accounts = self._get_accounts()['response']
                    data = []
                    for acc in accounts:
                        time.sleep(1)
                        data.append(self._get_statistic(date1, date2, acc['account_id']))
                        print(data)

                    return merge_dicts(*data)

                url = "ads.getStatistics", f"date_from={date1}&date_to={date2}&account_id={account_id}&ids_type={ids_type}&period={period}&ids={ids}&stats_fields=id"
                return self._use_method(*url)
            except Exception as e:
                logger.exception('Ошибка получения списока рекламных объявлений: ' + str(e))

        # Возвращает список рекламных кабинетов
        def _get_accounts(self):
            try:
                url = "ads.getAccounts", ""
                return self._use_method(*url)
            except Exception as e:
                logger.exception('Ошибка получения списока рекламных объявлений: ' + str(e))

        # Возвращает список рекламных объявлений
        def _get_ads(self, account_id):
            try:
                url = "ads.getAds", f"account_id={account_id}"
                return self._use_method(*url)
            except Exception as e:
                logger.exception('Ошибка получения списока рекламных объявлений: ' + str(e))

        # Возвращает список клиентов рекламного агентства.
        def _get_client(self, account_id):
            try:
                url = "ads.getClients", f"account_id={account_id}"
                return self._use_method(*url)
            except Exception as e:
                logger.exception('Ошибка получения клиентов рекламного агентства: ' + str(e))

        # Возвращает список кампаний рекламного кабинета
        def _get_campaigns(self, account_id):
            try:
                url = "ads.getCampaigns", f"account_id={account_id}"
                return self._use_method(*url)
            except Exception as e:
                logger.exception('Ошибка получения кампаний рекламного кабинета: ' + str(e))

        def _use_method(self, method, url_params):
            try:
                url = f'https://api.vk.com/method/{method}?access_token={self.access_token}&v={self.v}&{url_params}'

                response = requests.get(url).json()

                return response
            except Exception as e:
                logger.exception(f'Ошибка использования метода {method}: ' + str(e))

    class VK:

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
        def _get_statistic(self, date_from, date_to, metrics='all', id=None, company='ad_groups'):
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
                logger.exception('ВК статистика не получена: ' + str(e))

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
    clients = [['zawG5V96vFJJ1EJI',
                '7E1GWhpLPaCcHtmtayt5C7aPJWSVLx0S2X3C3SgA8BalOuYCZj9JhhE3bL6mEfuVovpj4JHK6OU74JGjk9RKsPjfhqzbuDzm1BgWMJ9kocc9gjPxA6MDChgVY4WFwfuhAtixAIqy4ZWwaRklGQJUbiGIP2w2f2syjea6Ru2gyV75LJR8kBf0riLaycaXM846s401zZhqWXJc831IYdR7AMSLzqvetRxEDMn0Du3PXo2ygUI1vAROOKu'],
               ]
    clients = [['vk1.a.hS8SS70suHSGGS7gyIW1RUwMmD0b2dmZNLCF5Xryw5-PRlkXBdBlB4kJbYpesHQCJBif6beMwefql_I5QxHuRJFXQyl_nnaq5MBt7toYOoNyVae__CpUrR3b398H62Z-YAX1Y5gP4pgdBKpUHZqRzwerWQP6MklDbQIcNh8H5VK_gkpeCfYv_OgM-8pBmSWPoSGfxU8pKtYmeOBfeeE5JA']]
    vks = VKs(clients)
    # static = vks.get_statistics_banners('2023-07-10', '2023-07-15')
    # vks.save_data_json('staticBaners')
    # static = vks.get_statistics_group_banners('2023-07-10', '2023-07-15')
    # vks.save_data_json('staticGroupBaber')
    # static = vks.get_statistics_ad_company('2023-07-10', '2023-07-15')
    # vks.save_data_json('staticAdCompany')
