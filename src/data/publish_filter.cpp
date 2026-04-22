// ============================================================================
// publish_filter.cpp — Stateful Publish Filter Implementation
// ============================================================================

#include "data/publish_filter.hpp"
#include <cmath>

namespace oro {

bool PublishFilter::should_publish(const TopicDescriptor& desc,
                                   float value,
                                   uint8_t state,
                                   int32_t ticks,
                                   uint64_t now_ms) {
    auto& st = state_[desc.topic_id < TOPIC_COUNT ? desc.topic_id : TOPIC_COUNT - 1];

    switch (desc.policy) {

    case PublishPolicy::ON_CHANGE: {
        // Digital sensors: publish only when state differs from last published
        if (!st.ever_published || st.last_digital_state != state) {
            st.last_digital_state   = state;
            st.last_publish_time_ms = now_ms;
            st.ever_published       = true;
            return true;
        }
        // Periodic fallback for topics with period_ms > 0
        if (desc.period_ms > 0 &&
            (now_ms - st.last_publish_time_ms) >= desc.period_ms) {
            st.last_publish_time_ms = now_ms;
            return true;
        }
        return false;
    }

    case PublishPolicy::ON_THRESHOLD: {
        // Analog sensors: publish when delta exceeds threshold
        if (!st.ever_published ||
            std::fabs(value - st.last_analog_value) >= desc.threshold) {
            st.last_analog_value    = value;
            st.last_publish_time_ms = now_ms;
            st.ever_published       = true;
            return true;
        }
        // Periodic fallback for slow sensors
        if (desc.period_ms > 0 &&
            (now_ms - st.last_publish_time_ms) >= desc.period_ms) {
            st.last_analog_value    = value;
            st.last_publish_time_ms = now_ms;
            return true;
        }
        return false;
    }

    case PublishPolicy::PERIODIC: {
        // Fixed-rate publishing with 10% timing tolerance to handle jitter
        if (!st.ever_published ||
            (now_ms - st.last_publish_time_ms) >= (desc.period_ms * 9 / 10)) {
            st.last_digital_state   = state;
            st.last_publish_time_ms = now_ms;
            st.ever_published       = true;
            return true;
        }
        return false;
    }

    case PublishPolicy::CONTINUOUS: {
        // Always publish (e.g., encoder while active)
        st.last_encoder_ticks   = ticks;
        st.last_publish_time_ms = now_ms;
        st.ever_published       = true;
        return true;
    }

    case PublishPolicy::ON_UPDATE: {
        // Publish whenever new data arrives (e.g., display, clock, LED)
        st.last_digital_state   = state;
        st.last_analog_value    = value;
        st.last_publish_time_ms = now_ms;
        st.ever_published       = true;
        return true;
    }

    }  // switch

    return false;  // Unreachable, but satisfies compiler
}

}  // namespace oro
