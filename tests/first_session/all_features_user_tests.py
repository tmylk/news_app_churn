from tests.abstract.all_features_user_tests import AllFeaturesUserTestsBaseClass
from first_session import EVENT_FEATURES

# TODO: Add proper first session unit tests. These ones are for events_all
class AllFeaturesUserTests(AllFeaturesUserTestsBaseClass):
    def setup():
        c = EVENT_FEATURES
