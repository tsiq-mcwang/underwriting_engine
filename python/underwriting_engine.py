#
# Copyright (c) 2018 Two Sigma Investments, LP.
# All Rights Reserved
#
# THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
# Two Sigma Investments, LP.
#
# The copyright notice above does not evidence any
# actual or intended publication of such source code.
#

"""
Code that underwrites a policy that goes through production underwriting
"""

from metrics.raw_data_wrappers.internal_logs.underwriting_log import UnderwritingLogLine
from pricing_risk.rating.rating_request_components import BOPRatingRequest
from pyaws.insure_service import get_insure_service, InsureService

import json
from enum import Enum
from contextlib import contextmanager
import uuid


class ExternalTenant(Enum):
    """
    The tenant (customer) to charge for these calls.

    The value of each of these enums is the cert in the cert header for that tenant.
    """
    ATTUNE = 'hamilton-guidewireprod-9999'
    BLACKBOARD = 'twosigma-9999'


class __PolicyUnderwriting(object):
    """
    Underwrites a policy
    """
    def __init__(self, service: InsureService):
        """
        Initialize the underwriting engine
        :param service:  Insure Service that used in production.
        """
        self.service = service

    def underwrite_policy(self, rating_request: BOPRatingRequest, tenant=ExternalTenant.ATTUNE) -> UnderwritingLogLine:
        """
        Underwrites a rating request and return underwriting response if not None
        :param rating_request: An input rating request
        :param tenant:  tenant to use for external calls.

        :return: UnderwritingLogLine containing both request and response, or None if there was an error in underwriting.
        """

        # create an underwriting request as the input to the underwriting engine
        # the underwritingData is obtained from the prefill outputs, and select the first set of values if multiple locations

        underwriting_data = {}

        for key, value in rating_request.commercial_BOP_policy.insured_locations[0].to_dict().items():
            if key not in ['ExternalVendorData', 'locationAddress', 'classifications', 'fipsApn']:
                underwriting_data[key] = value

        request = {
            'version': rating_request.version,
            'requestId': str(uuid.uuid4()),
            'quote': rating_request.quote.to_dict(),
            'effectiveDate': rating_request.commercial_BOP_policy.effective_date,
            'entity': {'address': rating_request.commercial_BOP_policy.insured_address.to_dict(),
                       'business': {'name': rating_request.commercial_BOP_policy.insured_name}},
            'underwritingData': underwriting_data
                       }

        # call the underwriting engine in IQS
        headers = {'Accept': 'application/vnd.uw-v1+json', 'Content-Type': 'application/vnd.uw-v1+json'}

        response = self.service.call_endpoint("uw", headers=headers, data=json.dumps(request))

        if response.status_code != 200:
            # ERROR
            return None

        json_line = {'request': request, 'response': response.json()}

        return UnderwritingLogLine(json_line)

    def _kill_service(self):
        """Kills the service"""
        self.service.kill_insure_service()


@contextmanager
def get_underwriting_engine(prod=False, kill_on_exit=True) -> __PolicyUnderwriting:
    """
    :param prod:    If True, runs the productions environment
    :param kill_on_exit: If True, kills the service if it exited
    :return:
    """

    service = get_insure_service(env="DEV" if not prod else "RATING_PROD")
    service_up = service.is_up()

    try:
        if not service_up:
            service.start_insure_service()
        yield __PolicyUnderwriting(service)
    finally:
        if kill_on_exit and (not service_up):
            service.kill_insure_service()



