import json
import time
from datetime import date

import requests
import logging

logger = logging

class VKs_old:
    # tokens это массив в котором лежат [token, token]
    def __init__(self, tokens):
        self.vks = []

        self.tokens = tokens
        self.data = None

        self.headers = 'ad_id, campaign_id, category1_id, category2_id, account_id, day, clicks, link_external_clicks, impressions, join_rate, reach, spent'
        self.date_from = None
        self.date_to = None

        self.start()

    # Запуск токенов
    def start(self):
        for token in self.tokens:
            vk = self.Vk_old(token)
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

    # Функция выдачи статистики для старого кабинета
    def get_statistics(self, date1, date2=date.today().strftime("%Y-%m-%d"), ids_type='office', ids=None):
        try:
            self.date_from = date1
            self.date_to = date2
            statics = []

            # собираем статичтику в массив
            for vk in self.vks:
                stats = vk._get_old_statistic(date_from=self.date_from, date_to=self.date_to, ids=ids,
                                              ids_type=ids_type)

                if stats:
                    statics.extend(stats)
                else:
                    logger.warning('По данному даипозону для токена ' + vk.access_token[:5] + "ничего не найденно")
            self.data = statics

            return self.headers, statics
        except Exception as e:
            logger.exception('Ошибка выдачи ститистики для старого кабинета: ' + str(e))

    # Доп функция для изменения ключа в словоре data
    def _prefix_dict(self, d, prefix):
        return {f"{prefix}_{key}": value for key, value in d.items()}

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

                                ads = self._merge_dicts(*ads)
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

                        data.append(
                            self._get_old_statistic(date_from, date_to, ids_ads, acc['account_id'], ids_type='ad'))

                    return self._refresh_and_merge(self._merge_dicts(*data)['response'], self.data)

                url = "ads.getStatistics", f"date_from={date_from}&date_to={date_to}&account_id={account_id}&ids_type={ids_type}&period={period}&ids={ids}&stats_fields=id"

                response = self._use_method(*url)

                return response

            except Exception as e:
                logger.exception('Ошибка получения списока рекламных объявлений: ' + str(e))

        # Возврашаем объедененные и преобразованные данные стастистики
        def _refresh_and_merge(self, data1, data2):
            return [
                [
                    ad['id'],
                    ad['campaign_id'],
                    ad['category1_id'],
                    ad['category2_id'],
                    k1,  # account_id
                    stat['day'],
                    stat.get('clicks', 0),
                    stat.get('link_external_clicks', 0),
                    stat.get('impressions', 0),
                    stat.get('join_rate', 0),
                    stat.get('reach', 0),
                    stat.get('spent', 0)
                ]
                for d1 in data1
                # проходимя по data1  если в d1 есть стаститика проверяем ид в data2 если все ок все соединяем
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

        # функция объединения json dict
        def _merge_dicts(self, *dicts):
            r = {}
            for d in dicts:
                for k, v in d.items():
                    r.setdefault(k, []).extend(v)
            return r


if __name__ == '__main__':
    tokens = [
        'vk1.a.hS8SS70suHSGGS7g32422D0b2dmZNLCF5Xryw5-PRlkXBdBlB4kJbYpesHQCJBif6beMwefql_I5QxHuRJFXQyl_nnaq5MBt7toYOoNyVae__CpUrR3b398H62Z-YAX1Y5gP4pgdBKpUHZqRzwerWQP6MklDbQIcNh8H5VK_gkpeCfYv_OgM-8pBmSWPoSGfxU8pKtYmeOBfeeE5JA']

    # @abolior - tg
    vks = VKs_old(tokens)
    print(vks.get_statistics('2022-06-10', '2023-01-01'))
