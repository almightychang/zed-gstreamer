// Drives zeddatamux from two streaming threads, as in the README pipeline where src_data is
// linked directly and src_left goes through a queue: video frame k enters after data frame k,
// and data frame k+1 arrives while video frame k is being muxed. Build with -fsanitize=thread
// and run under `setarch $(uname -m) -R` to see the race on the shared matching state.
#define PACKAGE "regression-test"
#define GST_PACKAGE_VERSION "test"
#define GST_PACKAGE_LICENSE "LGPL"
#define GST_PACKAGE_NAME "regression-test"
#define GST_PACKAGE_ORIGIN "local"
#include "gst-zed-data-mux/gstzeddatamux.cpp"

#include <atomic>
#include <chrono>
#include <thread>

static std::atomic<int> received{0};

static GstFlowReturn consume(GstPad *, GstObject *, GstBuffer *buf) {
    ++received;
    gst_buffer_unref(buf);
    return GST_FLOW_OK;
}

static GstBuffer *data_frame(guint64 k) {
    GstBuffer *buf = gst_buffer_new_allocate(NULL, sizeof(GstZedSrcMeta), NULL);
    gst_buffer_memset(buf, 0, 0, sizeof(GstZedSrcMeta));
    GST_BUFFER_PTS(buf) = k;
    return buf;
}

static GstBuffer *video_frame(guint64 k) {
    GstBuffer *buf = gst_buffer_new_allocate(NULL, 1920 * 1080 * 4, NULL);
    GST_BUFFER_PTS(buf) = k;
    return buf;
}

int main(int argc, char **argv) {
    gst_init(&argc, &argv);
    const int frames = 500;
    auto *mux = GST_ZEDDATAMUX(g_object_new(gst_zeddatamux_get_type(), NULL));
    GstPad *sink = gst_pad_new("consumer", GST_PAD_SINK);
    gst_pad_set_chain_function(sink, consume);
    gst_pad_set_active(sink, TRUE);
    gst_pad_link(mux->srcpad, sink);
    gst_element_set_state(GST_ELEMENT(mux), GST_STATE_PLAYING);
    gst_pad_push_event(mux->srcpad, gst_event_new_stream_start("test"));
    GstSegment segment;
    gst_segment_init(&segment, GST_FORMAT_TIME);
    gst_pad_push_event(mux->srcpad, gst_event_new_segment(&segment));

    // Data frame k+1 arrives 0-3 ms after video frame k entered the mux.
    std::atomic<int> video_entering{0};
    std::atomic<int> data_done{0};
    std::thread data_thread([&] {
        for (int k = 1; k <= frames; ++k) {
            while (video_entering.load() < k - 1) std::this_thread::yield();
            std::this_thread::sleep_for(std::chrono::microseconds(g_random_int_range(0, 3000)));
            gst_zeddatamux_chain_data(mux->sinkpad_data, GST_OBJECT(mux), data_frame(k));
            data_done.store(k);
        }
    });
    std::thread video_thread([&] {
        for (int k = 1; k <= frames; ++k) {
            while (data_done.load() < k) std::this_thread::yield();
            video_entering.store(k);
            gst_zeddatamux_chain_video(mux->sinkpad_video, GST_OBJECT(mux), video_frame(k));
        }
    });
    data_thread.join();
    video_thread.join();
    g_print("received %d/%d\n", received.load(), frames);

    gst_element_set_state(GST_ELEMENT(mux), GST_STATE_NULL);
    gst_object_unref(mux);
    gst_object_unref(sink);
    return 0;
}
