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
        self.new_vks = []
        self.old_vks = []

        self.clients = clients
        self.statics = None  # json_header
        self.new_data= None  # json_data
        self.old_data = None

        self.date_from = None
        self.date_to = None

        self.start()

    # Запуск токенов
    def start(self):
        for client in self.clients:
            if len(client) > 1:
                vk = self.VK_new(client[0], client[1])  # Создаем ВК объекты
                self.new_vks.append(vk)
            else:
                vk = self.Vk_old(client[0])
                self.old_vks.append(vk)

    # Проверка работоспособности токенов
    def validate(self):
        try:
            vks = *self.new_vks, *self.old_vks
            for vk in vks:
                if not vk._validate():
                    return False

            return True
        except Exception as e:
            logger.exception('Ошибка валидации данных: ' + str(e))

    # Функция сохранения data в json
    def save_data_json(self, name):

        with open(f'{name}.json', 'w', encoding='utf-8') as f:
            # Функция `dump` записывает объект data в открытый файл
            json.dump(self.new_data, f, ensure_ascii=False, indent=4)

    # Функция выдачи названий столбцов для старого кабинета
    def get_old_header(self):
        try:
            return 'ad_id, campaign_id, category1_id, category2_id, account_id, day, clicks, link_external_clicks, impressions, join_rate, reach, spent' # f"{', '.join(self.old_data[0].keys())}"
        except Exception as e:
            logger.exception('Ошибка выдачи названий столбцов для старого кабинета ' + str(e))

    # Функция выдачи названий столбцов для нового кабинета
    def get_new_header(self):
        try:

            return f"{', '.join(self.new_data[0].keys())}"
        except Exception as e:
            logger.exception('Ошибка выдачи названий столбцов для нового кабинета ' + str(e))

    # Функция выдачи статистики по 1 промежутку для разных кабинетов
    def get_all_statostics(self, date1, date2):
        try:

            if self.old_vks:
                self.old_data = self._get_old_statistics(date1, date2)

            if self.new_vks:
                self.new_data = self._get_statistics_banners(date1, date2)

            return self.new_data, self.old_data
        except Exception as e:
            logger.exception('Ошибка  выдачи статистики по 1 промежутку для разных кабинетов: ' + str(e))

    # Функция выдачи статистики по банерам
    def _get_statistics_banners(self, date1, date2, ids=None):
        try:
            data_banners = self._get_new_statistics(date1, date2, 'banners', ids)

            self.new_data = self._refresh_banners_included(data_banners)

            return self.new_data
        except Exception as e:
            logger.exception('Ошибка выдачи статистики по банерам: ' + str(e))


    # Функция добавления родителей к баннеру
    def _refresh_banners_included(self, data_banners):
        try:
            data = {}
            for vk in self.new_vks:
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
    def _get_new_statistics(self, date1, date2, company='ad_plans', id=None):
        try:
            self.date_from = date1
            self.date_to = date2
            statics = []
            # собираем статичтику в массив
            for vk in self.new_vks:
                logger.info('Работаю  с новым кабинетом ')
                stats = vk._get_statistic(date_from=self.date_from, date_to=self.date_to, company=company, id=id)

                if stats:
                    statics.append(stats)
            # объединяем все json в один
            statics = merge_dicts(*statics)
            new_data = self.get_data_from_hedears(statics)

            return new_data
        except Exception as e:
            logger.exception('Ошибка выдачи ститистики для нового кабинета: ' + str(e))

    # Функция выдачи статистики для старого кабинета
    def _get_old_statistics(self, date1, date2, ids_type='office', ids=None):
        try:
            self.date_from = date1
            self.date_to = date2
            statics = []

            # собираем статичтику в массив
            for vk in self.old_vks:
                stats = vk._get_old_statistic(date_from=self.date_from, date_to=self.date_to, ids=ids, ids_type=ids_type)

                if stats:
                    statics.extend(stats)
                else:
                    logger.warning('По данному даипозону для токена ' + vk.access_token[:5] + "ничего не найденно")
            self.old_data = statics

            return statics
        except Exception as e:
            logger.exception('Ошибка выдачи ститистики для старого кабинета: ' + str(e))

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
            self.data = {}

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

        # Возвращает статистику показателей эффективности по постам
        def _get_old_statistic(self, date_from, date_to, ids=None, account_id=None, ids_type='office', period='day'):
            try:
                if not ids:
                    accounts = self._get_accounts()['response']

                    data = []
                    for acc in accounts:
                        if acc['account_type'] == 'agency':
                            time.sleep(0.33)
                            users = self._get_client(acc['account_id'])['response']

                            if users:
                                ads = []
                                for user in users:
                                    time.sleep(0.33)
                                    ad = self._get_ads(acc['account_id'], client_id=user.get('id'))

                                    ads.append(ad)

                                ads = merge_dicts(*ads)
                            else:
                                continue

                        else:
                            time.sleep(0.33)
                            ads = self._get_ads(acc['account_id'])

                        if ads.get('error'):
                            logger.warning('В ads старого кабинета была найдена ошибка: ' + str(ads))
                            continue

                        self.data[str(acc["account_id"])] = ads['response']

                        ids_ads = [ad.get("id") for ad in ads['response']]

                        time.sleep(0.33)

                        data.append(self._get_old_statistic(date_from, date_to, ids_ads, acc['account_id'], ids_type='ad'))

                    return self._refresh_and_merge(merge_dicts(*data)['response'], self.data)

                url = "ads.getStatistics", f"date_from={date_from}&date_to={date_to}&account_id={account_id}&ids_type={ids_type}&period={period}&ids={ids}&stats_fields=id"

                response = self._use_method(*url)

                return response

            except Exception as e:
                logger.exception('Ошибка получения списока рекламных объявлений: ' + str(e))

        # Возврашаем объедененные и преобразованные данные стастистики
        def _refresh_and_merge(self, data1, data2):
            return [
                {
                    'ad_id': ad['id'],
                    'campaign_id': ad['campaign_id'],
                    'category1_id': ad['category1_id'],
                    'category2_id': ad['category2_id'],
                    'account_id': k1,
                    'day': stat['day'],
                    'clicks': stat.get('clicks', 0),
                    'link_external_clicks': stat.get('link_external_clicks', 0),
                    'impressions': stat.get('impressions', 0),
                    'join_rate': stat.get('join_rate', 0),
                    'reach': stat.get('reach', 0),
                    'spent': stat.get('spent', 0),
                }
                for d1 in data1 # проходимя по data1  если в d1 есть стаститика проверяем ид в data2 если все ок все соединяем
                if d1['stats']
                for k1, v1 in data2.items()
                for ad in v1
                if d1['id'] == ad['id']
                for stat in d1['stats']
            ]

        # Возвращает список рекламных кабинетов
        def _get_accounts(self):
            try:
                url = "ads.getAccounts", ""
                return self._use_method(*url)
            except Exception as e:
                logger.exception('Ошибка получения списока рекламных объявлений: ' + str(e))

        # Возвращает список рекламных объявлений
        def _get_ads(self, account_id, client_id=None):
            try:

                if client_id is not None:
                    account_id = str(account_id) + f"&client_id={str(client_id)}"

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
    clients = [['zawG5V96vFJJ1EJI',
                '7E1GWhpLPaCcHtmtayt5C7aPJWSVLx0S2X3C3SgA8BalOuYCZj9JhhE3bL6mEfuVovpj4JHK6OU74JGjk9RKsPjfhqzbuDzm1BgWMJ9kocc9gjPxA6MDChgVY4WFwfuhAtixAIqy4ZWwaRklGQJUbiGIP2w2f2syjea6Ru2gyV75LJR8kBf0riLaycaXM846s401zZhqWXJc831IYdR7AMSLzqvetRxEDMn0Du3PXo2ygUI1vAROOKu'],]
    ['vk1.a.hS8SS70suHSGGS7gyIW1RUwMmD0b2dmZNLCF5Xryw5-PRlkXBdBlB4kJbYpesHQCJBif6beMwefql_I5QxHuRJFXQyl_nnaq5MBt7toYOoNyVae__CpUrR3b398H62Z-YAX1Y5gP4pgdBKpUHZqRzwerWQP6MklDbQIcNh8H5VK_gkpeCfYv_OgM-8pBmSWPoSGfxU8pKtYmeOBfeeE5JA']
    vks = VKs(clients)
    print(vks.get_all_statostics('2023-08-24', '2023-08-26'))
    print(vks.get_old_header())
    print(vks.get_new_header())
