#ifndef DATA_PUBLISH_FILTER_HPP
#define DATA_PUBLISH_FILTER_HPP
// ============================================================================
// publish_filter.hpp — Stateful Publish Filter Engine
//
// Enforces per-topic publish policies (ON_CHANGE, ON_THRESHOLD, PERIODIC,
// CONTINUOUS, ON_UPDATE) with zero heap allocation on the hot path.
//
// Maintains a fixed std::array<TopicState, TOPIC_COUNT> of last-known
// values and timestamps. The should_publish() method evaluates the
// appropriate policy and returns true/false deterministically.
// ============================================================================

#include "topic_registry.hpp"
#include <array>
#include <cstdint>

namespace oro {

// ── Per-Topic Cached State ──────────────────────────────────────────────────

struct TopicState {
  float last_analog_value = 0.0f;
  uint8_t last_digital_state = 0xFF; // "never published" sentinel
  int32_t last_encoder_ticks = 0;
  uint64_t last_publish_time_ms = 0;
  bool ever_published = false;
};

// ── Publish Filter ──────────────────────────────────────────────────────────

class PublishFilter {
public:
  PublishFilter() = default;

  // Evaluate whether a message should be published for the given topic.
  // Updates internal state if publish is approved.
  /*
    @param desc   Topic descriptor from TOPIC_REGISTRY
    @param value  Analog value (used for ANALOG category)
    @param state  Digital state (used for DIGITAL category)
    @param ticks  Encoder ticks (used for ENCODER category)
    @param now_ms Current epoch milliseconds
    @return true if the message should be published
  */
  
  bool should_publish(const TopicDescriptor &desc, float value, uint8_t state,
                      int32_t ticks, uint64_t now_ms);

  // Check all topics for periodic fallback publish opportunities.
  // Returns a bitmask (or calls a callback) for topics due for publish.
  // The caller is responsible for constructing and sending the messages.
  //
  // @param now_ms Current epoch milliseconds
  // @param callback Function called with topic_id for each due topic
  template <typename Func>
  void check_periodic(uint64_t now_ms, Func &&callback) {
    for (uint8_t i = 0; i < TOPIC_COUNT; ++i) {
      const auto &desc = TOPIC_REGISTRY[i];
      if (desc.period_ms == 0)
        continue;

      auto &st = state_[i];
      if (!st.ever_published)
        continue; // Nothing to re-publish

      // Fallback re-publish if we haven't seen an update for 1.5x the expected
      // period
      if ((now_ms - st.last_publish_time_ms) >= (desc.period_ms * 3 / 2)) {
        st.last_publish_time_ms = now_ms;
        callback(i);
      }
    }
  }

  // Get the cached state for a topic (for diagnostics)
  const TopicState &get_state(uint8_t topic_id) const {
    return state_[topic_id < TOPIC_COUNT ? topic_id : TOPIC_COUNT - 1];
  }

private:
  std::array<TopicState, TOPIC_COUNT> state_{};
};

} // namespace oro
#endif // DATA_PUBLISH_FILTER_HPP
