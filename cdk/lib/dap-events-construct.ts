import { Construct } from "@aws-cdk/core";
import { Function } from '@aws-cdk/aws-lambda';
import { LambdaFunction } from '@aws-cdk/aws-events-targets';
import { Rule, RuleTargetInput, Schedule } from "@aws-cdk/aws-events";

export interface DAPFetchEventsProps {
    abc?: string;
}

export class DAPFetchEvents extends Construct {
    constructor(scope: Construct, id: string, targetFn: Function, props: DAPFetchEventsProps = {}) {
        super(scope, id);
        let fnName = 'minsa_vacunacion'
        let customPayload = {
            asset_url: 'https://cloud.minsa.gob.pe/s/ZgXoXqK2KLjRLxD/download',
            asset_filename: 'minsa_vacunacion.csv'
        }
        let eventTarget = new LambdaFunction(targetFn, {
            event: RuleTargetInput.fromObject(customPayload)
        })
        let eventRule = new Rule(this, `dap_trigger_event_${fnName}`,{
            schedule: Schedule.cron({ minute: '50', hour: '22' }),
            targets: [eventTarget]
        })
    }
}