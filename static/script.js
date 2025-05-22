$(function() {
  const source = new EventSource('/stream');

  source.onmessage = function(e) {
    const data = JSON.parse(e.data);

    // format and display timestamp
    const dt = new Date(data.timestamp);
    $('#last-update').text(dt.toLocaleString());

    // pick the highest-probability label
    const entries = Object.entries(data.result || {});
    const [bestLabel] = entries.reduce(
      (maxEntry, entry) => entry[1] > maxEntry[1] ? entry : maxEntry,
      ['', -Infinity]
    );

    $('#traffic-light')
      .text(`The traffic light showing right now is “${bestLabel}.”`);
  };

  source.onerror = function(err) {
    console.error('SSE error', err);
    source.close();
  };
});
