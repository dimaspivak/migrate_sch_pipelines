#!/usr/bin/env python3

# NOTE: This script requires setting environment variables SCH_SERVER_URL (e.g.
#       SCH_SERVER_URL='https://trailer.streamsetscloud.com'),
#       SCH_USERNAME (e.g. SCH_USERNAME='dima@engproductivity'), and SCH_PASSWORD (e.g. SCH_PASSWORD='abc123').

import argparse
import logging
import os

from streamsets.sdk import ControlHub

# The keys of this dictionary are current stages and their values are which stages to replace them with.
STAGE_MAPPINGS = {'Dev Raw Data Source': 'Dev Data Generator',
                  'Trash': 'Local FS'}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('pipeline', nargs='+', help='The name of one or more SCH pipelines to migrate')
    parser.add_argument('--verbose', '-v', action='store_true')
    args = parser.parse_args()

    logging.basicConfig(level='DEBUG' if args.verbose else 'INFO')
    logger = logging.getLogger('migrate_sch_pipelines')

    control_hub = ControlHub(server_url=os.getenv('SCH_SERVER_URL'),
                             username=os.getenv('SCH_USERNAME'),
                             password=os.getenv('SCH_PASSWORD'))

    # We convert the STAGE_MAPPINGS (above) into a form with which we can efficiently handle.
    _stage_mappings = {old.replace(' ', ''): new for old, new in STAGE_MAPPINGS.items()}

    for pipeline_name in args.pipeline:
        logger.info('Processing pipeline %s ...', pipeline_name)
        pipeline = control_hub.pipelines.get(name=pipeline_name)

        stages_to_add = []
        for i, stage in enumerate(pipeline._pipeline_definition['stages']):
            stage_instance_label = stage['instanceName'].split('_', 1)[0]
            new_stage = _stage_mappings.get(stage_instance_label)
            if new_stage:
                input_lanes = stage['inputLanes']
                output_lanes = stage['outputLanes']
                stages_to_add.append({'label': new_stage,
                                      'index': i,
                                      'input_lanes': input_lanes,
                                      'output_lanes': output_lanes})

        # To avoid issues caused by lists shifting as earlier elements are deleted, we'll delete entries from the
        # right.
        for i in (stage['index'] for stage in reversed(stages_to_add)):
            logger.info('Deleting stage %s ...', pipeline._pipeline_definition['stages'][i]['instanceName'])
            del pipeline._pipeline_definition['stages'][i]

        # Use the same authoring Data Collector for the new pipeline as for the original.
        data_collector = control_hub.data_collectors.get(id=pipeline.sdc_id)
        pipeline_builder = control_hub.get_pipeline_builder(data_collector=data_collector)
        # A bit of a hack to handle the fact that we normally import pipelines exported through the UI.
        pipeline_builder.import_pipeline({'pipelineConfig': pipeline._pipeline_definition,
                                          'pipelineRules': pipeline._rules_definition})

        for stage in stages_to_add:
            new_stage = pipeline_builder.add_stage(stage['label'])
            pipeline_builder._pipeline['pipelineConfig']['stages'].remove(new_stage._data)
            pipeline_builder._pipeline['pipelineConfig']['stages'].insert(stage['index'], new_stage._data)
            new_stage._data['inputLanes'] = stage['input_lanes']
            new_stage._data['outputLanes'] = stage['output_lanes']

        new_pipeline = pipeline_builder.build(f'{pipeline.name}_2')

        control_hub.publish_pipeline(new_pipeline, commit_message=f'Automated migration of {pipeline_name}')
        logger.info('Successfully migrated pipeline %s (new name: %s)', pipeline_name, new_pipeline.name)

if __name__ == '__main__':
    main()
