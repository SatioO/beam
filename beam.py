from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
import logging
import argparse
import re
import apache_beam as beam


class WordExtractingDoFn(beam.DoFn):
    def __init__(self):
        self.words_counter = Metrics.counter(self.__class__, 'words')

    def process(self, element):
        text_line = element.strip()
        words = re.findall(r'[\w\']+', text_line, re.UNICODE)
        
        for word in words:
            self.words_counter.inc()
        
        return words

def count_ones(word_count):
    
    return word_count
    
def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest='input', help='Input file to process')
    parser.add_argument('--output', dest='output',
                        help='Output file to write results')
    args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    p = beam.Pipeline(options=pipeline_options)

    lines = p | 'read' >> ReadFromText(args.input)
    words = (lines 
                | 'split' >> beam.ParDo(WordExtractingDoFn())
                | 'map' >> beam.Map(lambda x: (x, 1))
                | 'group' >> beam.GroupByKey()
                | 'count' >> beam.Map(lambda (word, ones): (word, sum(ones)))
            )

    words | 'write' >> WriteToText(args.output)

    result = p.run()
    result.wait_until_finish()

    if not hasattr(result, 'has_job'):
        words = MetricsFilter().with_name('words')
        query_result = result.metrics().query(words)
        print(query_result['counters'][0].result)

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
