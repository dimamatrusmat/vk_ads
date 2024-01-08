import requests
from time import sleep

logger = get_logger(__name__)

class GetDataMetrica:
    base_url = f'https://api-metrika.yandex.net/management/v1/counter/'
    FIELDS = {"YM": {"download": [
                            "ym:s:visitID", "ym:s:counterID", "ym:s:watchIDs", "ym:s:date", "ym:s:dateTime", "ym:s:dateTimeUTC",
                            "ym:s:isNewUser", "ym:s:startURL", "ym:s:endURL", "ym:s:pageViews", "ym:s:visitDuration", "ym:s:bounce",
                            "ym:s:ipAddress", "ym:s:regionCountry", "ym:s:regionCity", "ym:s:regionCountryID", "ym:s:regionCityID",
                            "ym:s:clientID", "ym:s:counterUserIDHash", "ym:s:networkType", "ym:s:goalsID", "ym:s:goalsSerialNumber",
                            "ym:s:goalsDateTime", "ym:s:goalsPrice", "ym:s:goalsOrder", "ym:s:goalsCurrency", "ym:s:lastTrafficSource",
                            "ym:s:lastAdvEngine", "ym:s:lastReferalSource", "ym:s:lastSearchEngineRoot", "ym:s:lastSearchEngine",
                            "ym:s:lastSocialNetwork", "ym:s:lastSocialNetworkProfile", "ym:s:referer", "ym:s:lastDirectClickOrder",
                            "ym:s:lastDirectBannerGroup", "ym:s:lastDirectClickBanner", "ym:s:lastDirectClickOrderName",
                            "ym:s:lastClickBannerGroupName", "ym:s:lastDirectClickBannerName", "ym:s:lastDirectPhraseOrCond",
                            "ym:s:lastDirectPlatformType", "ym:s:lastDirectPlatform", "ym:s:lastDirectConditionType", "ym:s:lastCurrencyID",
                            "ym:s:lastUTMCampaign", "ym:s:lastUTMContent", "ym:s:lastUTMMedium", "ym:s:lastUTMSource", "ym:s:lastUTMTerm",
                             "ym:s:hasGCLID", "ym:s:lastGCLID", "ym:s:browserLanguage", "ym:s:browserCountry",
                            "ym:s:deviceCategory", "ym:s:mobilePhone", "ym:s:mobilePhoneModel",
                            "ym:s:operatingSystemRoot", "ym:s:operatingSystem", "ym:s:browser",
                            "ym:s:browserMajorVersion", "ym:s:browserMinorVersion", "ym:s:browserEngine", "ym:s:browserEngineVersion1",
                            "ym:s:browserEngineVersion2", "ym:s:browserEngineVersion3", "ym:s:browserEngineVersion4", "ym:s:cookieEnabled",
                            "ym:s:javascriptEnabled", "ym:s:screenFormat", "ym:s:screenColors", "ym:s:screenOrientation", "ym:s:screenWidth",
                            "ym:s:screenHeight", "ym:s:physicalScreenWidth", "ym:s:physicalScreenHeight", "ym:s:windowClientWidth",
                            "ym:s:windowClientHeight", "ym:s:purchaseID", "ym:s:purchaseDateTime", "ym:s:purchaseAffiliation",
                            "ym:s:purchaseRevenue", "ym:s:purchaseTax", "ym:s:purchaseShipping", "ym:s:purchaseCoupon",
                            "ym:s:purchaseCurrency", "ym:s:purchaseProductQuantity", "ym:s:productsPurchaseID", "ym:s:productsID",
                            "ym:s:productsName", "ym:s:productsBrand", "ym:s:productsCategory", "ym:s:productsCategory1",
                            "ym:s:productsCategory2", "ym:s:productsCategory3", "ym:s:productsCategory4", "ym:s:productsCategory5",
                            "ym:s:productsVariant", "ym:s:productsPosition", "ym:s:productsPrice", "ym:s:productsCurrency",
                            "ym:s:productsCoupon", "ym:s:productsQuantity", "ym:s:impressionsURL", "ym:s:impressionsDateTime",
                            "ym:s:impressionsProductID", "ym:s:impressionsProductName", "ym:s:impressionsProductBrand",
                            "ym:s:impressionsProductCategory", "ym:s:impressionsProductCategory1", "ym:s:impressionsProductCategory2",
                            "ym:s:impressionsProductCategory3", "ym:s:impressionsProductCategory4", "ym:s:impressionsProductCategory5",
                            "ym:s:impressionsProductVariant", "ym:s:impressionsProductPrice", "ym:s:impressionsProductCurrency",
                            "ym:s:impressionsProductCoupon", "ym:s:offlineCallTalkDuration", "ym:s:offlineCallHoldDuration",
                            "ym:s:offlineCallMissed", "ym:s:offlineCallTag", "ym:s:offlineCallFirstTimeCaller", "ym:s:offlineCallURL",
                            "ym:s:parsedParamsKey1", "ym:s:parsedParamsKey2", "ym:s:parsedParamsKey3", "ym:s:parsedParamsKey4",
                            "ym:s:parsedParamsKey5", "ym:s:parsedParamsKey6", "ym:s:parsedParamsKey7", "ym:s:parsedParamsKey8",
                            "ym:s:parsedParamsKey9", "ym:s:parsedParamsKey10", "ym:s:lastRecommendationSystem", "ym:s:lastMessenger"
                        ],

              "create_table": {
                                'ym:s:visitID': 'UInt64',
                                'ym:s:counterID': 'UInt32',
                                'ym:s:watchIDs': 'Array(UInt64)',
                                'ym:s:date': 'Date',
                                'ym:s:dateTime': 'DateTime64(3)',
                                'ym:s:dateTimeUTC': 'DateTime64(3)',
                                'ym:s:isNewUser': 'Bool',
                                'ym:s:startURL': 'String',
                                'ym:s:endURL': 'String',
                                'ym:s:pageViews': 'UInt32',
                                'ym:s:visitDuration': 'UInt32',
                                'ym:s:bounce': 'Bool',
                                'ym:s:ipAddress': 'String',
                                'ym:s:regionCountry': 'String',
                                'ym:s:regionCity': 'String',
                                'ym:s:regionCountryID': 'UInt32',
                                'ym:s:regionCityID': 'UInt32',
                                'ym:s:clientID': 'UInt64',
                                'ym:s:counterUserIDHash': 'UInt64',
                                'ym:s:networkType': 'LowCardinality(String)',
                                'ym:s:goalsID': 'Array(UInt32)',
                                'ym:s:goalsSerialNumber': 'Array(UInt32)',
                                'ym:s:goalsDateTime': 'Array(DateTime)',
                                'ym:s:goalsPrice': 'Array(Int64)',
                                'ym:s:goalsOrder': 'Array(String)',
                                'ym:s:goalsCurrency': 'LowCardinality(String)',
                                'ym:s:lastTrafficSource': 'LowCardinality(String)',
                                'ym:s:lastAdvEngine': 'LowCardinality(String)',
                                'ym:s:lastReferalSource': 'String',
                                'ym:s:lastSearchEngineRoot': 'LowCardinality(String)',
                                'ym:s:lastSearchEngine': 'LowCardinality(String)',
                                'ym:s:lastSocialNetwork': 'LowCardinality(String)',
                                'ym:s:lastSocialNetworkProfile': 'String',
                                'ym:s:referer': 'String',
                                'ym:s:lastDirectClickOrder': 'Nullable(UInt32)',
                                'ym:s:lastDirectBannerGroup': 'Nullable(UInt32)',
                                'ym:s:lastDirectClickBanner': 'LowCardinality(String)',
                                'ym:s:lastDirectClickOrderName': 'LowCardinality(String)',
                                'ym:s:lastClickBannerGroupName': 'LowCardinality(String)',
                                'ym:s:lastDirectClickBannerName': 'LowCardinality(String)',
                                'ym:s:lastDirectPhraseOrCond': 'String',
                                'ym:s:lastDirectPlatformType': 'LowCardinality(String)',
                                'ym:s:lastDirectPlatform': 'String',
                                'ym:s:lastDirectConditionType': 'LowCardinality(String)',
                                'ym:s:lastCurrencyID': 'LowCardinality(String)',
                                'ym:s:lastUTMCampaign': 'LowCardinality(String)',
                                'ym:s:lastUTMContent': 'LowCardinality(String)',
                                'ym:s:lastUTMMedium': 'LowCardinality(String)',
                                'ym:s:lastUTMSource': 'LowCardinality(String)',
                                'ym:s:lastUTMTerm': 'String',
                                'ym:s:hasGCLID': 'Bool',
                                'ym:s:lastGCLID': 'String',
                                'ym:s:browserLanguage': 'String',
                                'ym:s:browserCountry': 'String',
                                'ym:s:deviceCategory': 'LowCardinality(String)',
                                'ym:s:mobilePhone': 'String',
                                'ym:s:mobilePhoneModel': 'String',
                                'ym:s:operatingSystemRoot': 'LowCardinality(String)',
                                'ym:s:operatingSystem': 'String',
                                'ym:s:browser': 'String',
                                'ym:s:browserMajorVersion': 'UInt16',
                                'ym:s:browserMinorVersion': 'UInt16',
                                'ym:s:browserEngine': 'LowCardinality(String)',
                                'ym:s:browserEngineVersion1': 'UInt16',
                                'ym:s:browserEngineVersion2': 'UInt16',
                                'ym:s:browserEngineVersion3': 'UInt16',
                                'ym:s:browserEngineVersion4': 'UInt16',
                                'ym:s:cookieEnabled': 'Bool',
                                'ym:s:javascriptEnabled': 'Bool',
                                'ym:s:screenFormat': 'String',
                                'ym:s:screenColors': 'UInt8',
                                'ym:s:screenOrientation': 'LowCardinality(String)',
                                'ym:s:screenWidth': 'UInt16',
                                'ym:s:screenHeight': 'UInt16',
                                'ym:s:physicalScreenWidth': 'UInt16',
                                'ym:s:physicalScreenHeight': 'UInt16',
                                'ym:s:windowClientWidth': 'UInt16',
                                'ym:s:windowClientHeight': 'UInt16',
                                'ym:s:purchaseID': 'Array(String)',
                                'ym:s:purchaseDateTime': 'Array(DateTime)',
                                'ym:s:purchaseAffiliation': 'Array(String)',
                                'ym:s:purchaseRevenue': 'Array(Float64)',
                                'ym:s:purchaseTax': 'Array(Float64)',
                                'ym:s:purchaseShipping': 'Array(Float64)',
                                'ym:s:purchaseCoupon': 'Array(String)',
                                'ym:s:purchaseCurrency': 'Array(LowCardinality(String))',
                                'ym:s:purchaseProductQuantity': 'Array(Int64)',
                                'ym:s:productsPurchaseID': 'Array(String)',
                                'ym:s:productsID': 'Array(String)',
                                'ym:s:productsName': 'Array(String)',
                                'ym:s:productsBrand': 'Array(String)',
                                'ym:s:productsCategory': 'Array(String)',
                                'ym:s:productsCategory1': 'Array(String)',
                                'ym:s:productsCategory2': 'Array(String)',
                                'ym:s:productsCategory3': 'Array(String)',
                                'ym:s:productsCategory4': 'Array(String)',
                                'ym:s:productsCategory5': 'Array(String)',
                                'ym:s:productsVariant': 'Array(String)',
                                'ym:s:productsPosition': 'Array(Int32)',
                                'ym:s:productsPrice': 'Array(Float64)',
                                'ym:s:productsCurrency': 'Array(LowCardinality(String))',
                                'ym:s:productsCoupon': 'Array(String)',
                                'ym:s:productsQuantity': 'Array(Int32)',
                                'ym:s:impressionsURL': 'Array(String)',
                                'ym:s:impressionsDateTime': 'Array(DateTime)',
                                'ym:s:impressionsProductID': 'Array(String)',
                                'ym:s:impressionsProductName': 'Array(String)',
                                'ym:s:impressionsProductBrand': 'Array(String)',
                                'ym:s:impressionsProductCategory': 'Array(String)',
                                'ym:s:impressionsProductCategory1': 'Array(String)',
                                'ym:s:impressionsProductCategory2': 'Array(String)',
                                'ym:s:impressionsProductCategory3': 'Array(String)',
                                'ym:s:impressionsProductCategory4': 'Array(String)',
                                'ym:s:impressionsProductCategory5': 'Array(String)',
                                'ym:s:impressionsProductVariant': 'Array(String)',
                                'ym:s:impressionsProductPrice': 'Array(Float64)',
                                'ym:s:impressionsProductCurrency': 'Array(LowCardinality(String))',
                                'ym:s:impressionsProductCoupon': 'Array(String)',
                                'ym:s:offlineCallTalkDuration': 'Array(Int32)',
                                'ym:s:offlineCallHoldDuration': 'Array(Int32)',
                                'ym:s:offlineCallMissed': 'Array(Int32)',
                                'ym:s:offlineCallTag': 'Array(String)',
                                'ym:s:offlineCallFirstTimeCaller': 'Array(UInt32)',
                                'ym:s:offlineCallURL': 'Array(String)',
                                'ym:s:parsedParamsKey1': 'Array(String)',
                                'ym:s:parsedParamsKey2': 'Array(String)',
                                'ym:s:parsedParamsKey3': 'Array(String)',
                                'ym:s:parsedParamsKey4': 'Array(String)',
                                'ym:s:parsedParamsKey5': 'Array(String)',
                                'ym:s:parsedParamsKey6': 'Array(String)',
                                'ym:s:parsedParamsKey7': 'Array(String)',
                                'ym:s:parsedParamsKey8': 'Array(String)',
                                'ym:s:parsedParamsKey9': 'Array(String)',
                                'ym:s:parsedParamsKey10': 'Array(String)',
                                'ym:s:lastRecommendationSystem': 'String',
                                'ym:s:lastMessenger': 'String',
                                'YearNumber': 'Int32',
                                'MonthNumber': 'Int32',
                                'DayOfMonth': 'Int32',
                                'DayOfWeek': 'Int32',
                                'DayOfYear': 'Int32',
                                'WeekOfYear': 'Int32',
                                'Quarter': 'Int32',
                                'DayOfWeekString': """Enum8(
                                                            'Понедельник' = 1, 'Вторник' = 2, 'Среда' = 3, 
                                                            'Четверг' = 4, 'Пятница' = 5, 
                                                            'Суббота' = 6, 'Воскресенье' = 7, 
                                                            'Неизвестно' = 8
                                                        )"""},
              "order_by": ['`ym:s:date`', '`ym:s:dateTime`', '`ym:s:regionCity`', '`ym:s:lastTrafficSource`','`ym:s:lastDirectClickOrderName`','`ym:s:lastUTMCampaign`'],
              "partition_by": ['toYYYYMM(`ym:s:date`)']
              }}



    def __init__(self, OAuth, counterId):
        self.OAuth = OAuth  #токен
        self.counterId = counterId  #номер счетчика
        self.headers_metrica = {"Authorization": f"OAuth {self.OAuth}"}  #заголовки для запроса
        self.fields_for_query = ','.join(self.FIELDS['YM']["download"])



    def validate(self):
        try:
            response = requests.get(f'https://api-metrika.yandex.net/management/v1/counter/{self.counterId}/goals', headers=self.headers_metrica)
            if response.status_code == 200:
                logger.info(f'Валидация к счетчику: {self.counterId} прошла успешно.')
                return True
            else:
                logger.error(f'Ошибка при проверке данных к счетчику: {self.counterId}.')
                raise ValueError('Проверьте токен и номер счетчика!')
        except Exception as e:
            logger.exception(e)


    #функция отправки запроса

    def _send_request(self, endpoint, method='GET',stage='INFO'):
        try:
            url = f'{self.base_url}{self.counterId}/{endpoint}'
            if method == 'GET':
                response = requests.get(url, headers=self.headers_metrica)
            elif method == 'POST':
                response = requests.post(url, headers=self.headers_metrica)
            # Добавьте другие методы по необходимости
            #response.raise_for_status()  # Вызовет исключение для кодов состояния HTTP 4xx/5xx

            if stage == 'INFO':
                data = response.json()
                if 'errors' in data:
                    raise Exception(f"Ошибка при формировании отчета: {data['errors'][0]['message']}")
                return response.json()
            elif stage == 'GET_DATA':
                rows = response.content.decode('utf-8').split('\n')[1:-1]  # Убираем заголовки и последнюю пустую строку
                return [row.split('\t') for row in rows]  # Разделяем строки по табуляции и возвращаем список
        except Exception as e:
            logger.error('Ошибка при отправке запроса.')
            logger.exception(e)



    #получение списка запросов
    def get_list_of_queries(self):
        try:
            return self._send_request('logrequests','GET')["requests"]
        except Exception as e:
            logger.exception(e)

    #удаление ранее созданного запроса
    def delete_query(self):
        try:
            self._send_request(f'logrequest/{self.request_id_for_delete}/cancel', 'POST')
        except Exception as e:
            logger.exception(e)

    #отмена не готового запроса
    def clean_query(self):
        try:
            self._send_request(f'logrequest/{self.request_id_for_delete}/clean', 'POST')
        except Exception as e:
            logger.exception(e)

    #удаление старых и неготовых запросов
    def delete_old_queries(self):
        try:
            list_of_queries = self.get_list_of_queries()  # список запросов
            if len(list_of_queries) == 0:  #проверка на пустой список запросов
                return
            else:
                for query in list_of_queries:  # Итерация непосредственно по элементам списка

                    status = query['status']
                    self.request_id_for_delete = query['request_id']

                    if status == 'processed':
                        self.clean_query()
                    elif status == 'created':
                        self.delete_query()
                    elif status == 'awaiting_retry':
                        self.clean_query()

        except Exception as e:
            logger.exception(e)


    #оценка возможности создания запроса
    def can_create_query(self):
        try:
            logger.info('Получил ответ возможности создания отчета.')
            return self._send_request(f'logrequests/evaluate?{self.params}','GET')['log_request_evaluation']['possible']
        except Exception as e:
            logger.exception(e)

    #отправка запроса
    def create_query(self):
        try:
            return self._send_request(f'logrequests?{self.params}','POST')['log_request']['request_id']
        except Exception as e:
            logger.exception(e)

    #получение статуса запроса
    def get_log_request_info_status(self):
        try:
            logger.info('Получил статус отчета.')
            return self._send_request(f'logrequest/{self.request_id}', 'GET')['log_request']['status']
        except Exception as e:
            logger.exception(e)

    # получение количества частей
    def get_log_request_info_parts(self):
        try:
            return len(self._send_request(f'logrequest/{self.request_id}', 'GET')['log_request']['parts'])
        except Exception as e:
            logger.exception(e)

    #получение данных частями
    def get_one_part_data(self,part):
        try:
            return self._send_request(f'logrequest/{self.request_id}/part/{part}/download','GET','GET_DATA')
        except Exception as e:
            logger.exception(e)


    #ожидание подготовки отчета с прохождение по датам
    def waiting_for_report(self,date1,date2,logs_source_type):
        '''
        возвращает True когда отчет готов
        '''
        try:
            self.date1 = date1
            self.date2 = date2
            self.source_type = logs_source_type
            self.params = f'date1={self.date1}&date2={self.date2}&fields={self.fields_for_query}&source={self.source_type}'

            self.delete_old_queries()

            if self.can_create_query() == True:
                self.request_id = self.create_query()

                sleep(5)

                while True:
                    status = self.get_log_request_info_status()

                    if status == 'created':
                        logger.info('Отчет не готов.')
                        sleep(30)
                    elif status in ['processing_failed', 'awaiting_retry']:
                        # В случае ошибок возвращаем False

                        logger.error(f'Ошибка при получении данных. Статус {status}')
                        return False
                    elif status == 'processed':
                        logger.info('Отчет готов к выгрузке.')
                        return True

            else:
                logger.error('Проверьте параметры запроса.')
        except Exception as e:
            logger.exception(e)


    #получение всех данных в виде списка списков
    def get_all_parts(self):
        try:
            parts = self.get_log_request_info_parts()
            logger.info(f'Всего частей в отчете: {parts}')
            all_data = []
            for part in range(parts):
                part_data = self.get_one_part_data(part)
                all_data.extend(part_data)  # Используйте extend вместо append
            return all_data  # Возвращаем список с данными всех частей
        except Exception as e:
            logger.exception(e)

    #получение заголовков
    def get_headers(self):
        try:
            return list(self.FIELDS['YM']['download'])
        except Exception as e:
            logger.exception(e)

    def format_headers_and_rows(self):
        try:
            headers = self.get_headers()  # Получаем заголовки
            all_data = self.get_all_parts()  # Получаем все данные

            return headers, all_data  # Возвращаем заголовки и строки данных
        except Exception as e:
            logger.exception(e)
            return [], []











