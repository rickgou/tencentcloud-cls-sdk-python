# -*- coding: utf-8 -*-


import logging
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Thread

from tencentcloud.log.consumer.consumer_client import ConsumerClient
from tencentcloud.log.consumer.heart_beat import ConsumerHeatBeat
from tencentcloud.log.consumer.partition_worker import PartitionConsumerWorker


class ConsumerWorkerLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        consumer_worker = self.extra['consumer_worker']  # type: ConsumerWorker
        consumer_option = consumer_worker.option
        _id = '/'.join([
            consumer_option.logset_id, str(consumer_option.topic_ids),
            consumer_option.consumer_group_name, consumer_option.consumer_name
        ])
        return "[{0}] {1}".format(_id, msg), kwargs


class ConsumerWorker(Thread):
    def __init__(self, make_processor, consumer_option, args=None,
                 kwargs=None):
        super(ConsumerWorker, self).__init__()
        self.make_processor = make_processor
        self.process_args = args or ()
        self.process_kwargs = kwargs or {}
        self.option = consumer_option
        self.consumer_client = \
            ConsumerClient(consumer_option.endpoint, consumer_option.access_key_id, consumer_option.access_key,
                           consumer_option.logset_id, consumer_option.topic_ids, consumer_option.consumer_group_name,
                           consumer_option.consumer_name, consumer_option.internal, consumer_option.region)
        self.shut_down_flag = False
        self.logger = ConsumerWorkerLoggerAdapter(
            logging.getLogger(__name__), {"consumer_worker": self})
        self.partition_consumers = {}

        self.last_owned_consumer_finish_time = 0

        self.consumer_client.create_consumer_group(consumer_option.consumer_group_time_out)
        self.heart_beat = ConsumerHeatBeat(self.consumer_client, consumer_option.topic_ids,
                                           consumer_option.heartbeat_interval,
                                           consumer_option.consumer_group_time_out)

        if consumer_option.partition_executor is not None:
            self.own_executor = False
            self._executor = consumer_option.partition_executor
        else:
            self.own_executor = True
            self._executor = ThreadPoolExecutor(max_workers=consumer_option.worker_pool_size)

    @property
    def executor(self):
        return self._executor

    def _need_stop(self):
        if not self.option.offset_end_time:
            return False

        all_finish = True
        for partition_id, consumer in self.partition_consumers.items():
            if consumer.is_shutdown():
                continue

            # has not yet do any successful fetch yet or get some data
            if (consumer.last_success_fetch_time == 0 or consumer.last_fetch_count > 0) and \
                    not consumer.next_task_reach_end:
                return False

        # init self.last_owned_consumer_finish_time if it's None
        if all_finish and self.last_owned_consumer_finish_time == 0:
            self.last_owned_consumer_finish_time = time.time()

        if abs(time.time() - self.last_owned_consumer_finish_time) >= \
                self.option.consumer_group_time_out + self.option.heartbeat_interval:
            return True

        return False

    def run(self):
        self.logger.info('consumer worker "{0}" start '.format(self.option.consumer_name))
        self.heart_beat.start()

        while not self.shut_down_flag:
            held_partitions = self.heart_beat.get_held_partitions()

            last_fetch_time = time.time()
            for partitions in held_partitions:
                partition_ids = partitions['Partitions']
                topic_id = partitions['TopicID']
                if self.shut_down_flag:
                    break

                for partition_id in partition_ids:
                    partition_consumer = self.get_partition_consumer(topic_id, partition_id)
                    if partition_consumer is None:  # error when init consumer. shutdown directly
                        self.shutdown()
                        break

                    partition_consumer.consume()

            self.clean_partition_consumer(held_partitions)

            if self._need_stop():
                self.logger.info(
                    "all owned partitions complete the tasks, owned partitions: {0}".format(self.partition_consumers))
                self.shutdown()

            time_to_sleep = self.option.data_fetch_interval - (time.time() - last_fetch_time)
            while time_to_sleep > 0 and not self.shut_down_flag:
                time.sleep(min(time_to_sleep, 1))
                time_to_sleep = self.option.data_fetch_interval - (time.time() - last_fetch_time)

        # # stopping worker, need to cleanup all existing partition consumer
        self.logger.info('consumer worker "{0}" try to cleanup consumers'.format(self.option.consumer_name))
        self.shutdown_and_wait()

        if self.own_executor:
            self.logger.info('consumer worker "{0}" try to shutdown executors'.format(self.option.consumer_name))
            self._executor.shutdown()
            self.logger.info('consumer worker "{0}" stopped'.format(self.option.consumer_name))
        else:
            self.logger.info('executor is shared, consumer worker "{0}" stopped'.format(self.option.consumer_name))

    def start(self, join=False):
        """
        when calling with join=True, must call it in main thread, or else, the Keyboard Interrupt won't be caputured.
        :param join: default False, if hold on until the worker is stopped by Ctrl+C or other reasons.
        :return:
        """
        Thread.start(self)

        if join:
            try:
                while self.is_alive():
                    self.join(timeout=60)
                self.logger.info("worker {0} exit unexpected, try to shutdown it".format(self.option.consumer_name))
                self.shutdown()
            except KeyboardInterrupt:
                self.logger.info("*** try to exit **** ")
                self.shutdown()

    def shutdown_and_wait(self):
        while True:
            time.sleep(0.5)
            for topic_partition, consumer in self.partition_consumers.items():
                if not consumer.is_shutdown():
                    consumer.shut_down()
                    break  # there's live consumer, no need to check, loop to next
            else:
                break  # all are shutdown, exit look

        self.partition_consumers.clear()

    def clean_partition_consumer(self, owned_partitions):
        remove_partitions = []
        # remove the partitions that's not assigned by server
        for topic_partition, consumer in self.partition_consumers.items():
            topic_id, partition_id = topic_partition.split(':')
            if not self.partition_in_owned(topic_id, int(partition_id), owned_partitions):
                self.logger.info('Try to call shut down for unassigned consumer partition: ' + str(topic_partition))
                consumer.shut_down()
                self.logger.info('Complete call shut down for unassigned consumer partition: ' + str(topic_partition))
            if consumer.is_shutdown():
                self.logger.info('Remove an unassigned consumer partition:' + str(topic_partition))
                self.heart_beat.remove_heart_partition(topic_id, int(partition_id))
                remove_partitions.append(topic_partition)

        for topic_partition in remove_partitions:
            self.partition_consumers.pop(topic_partition)

    @staticmethod
    def partition_in_owned(topic_id, partition_id, owned_partitions):
        include = False
        for partition_info in owned_partitions:
            if partition_info["TopicID"] == topic_id:
                include = True
                return partition_id in partition_info["Partitions"]
        if not include:
            return False

    def shutdown(self):
        self.shut_down_flag = True
        self.heart_beat.shutdown()
        self.logger.info('get stop signal, start to stop consumer worker "{0}"'.format(self.option.consumer_name))

    def get_partition_consumer(self, topic_id, partition_id):
        key = '{}:{}'.format(topic_id, partition_id)
        consumer = self.partition_consumers.get(key, None)
        if consumer is not None:
            return consumer

        try:
            processer = self.make_processor(*self.process_args, **self.process_kwargs)
        except Exception as ex:
            self.logger.error("fail to init processor {0} with parameters {1}, {2}, detail: {3}".format(
                self.make_processor, self.process_args, self.process_kwargs, ex, exc_info=True))
            return None

        consumer = PartitionConsumerWorker(self.consumer_client, topic_id, partition_id, self.option.consumer_name,
                                           processer, self.option.offset_start_time,
                                           max_fetch_log_group_size=self.option.max_fetch_log_group_size,
                                           executor=self._executor,
                                           offset_end_time=self.option.offset_end_time)
        self.partition_consumers[key] = consumer
        return consumer
