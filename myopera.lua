local url_count = 0


wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]

  -- Don't go to url templates
  if string.match(url, "{{") then
    return false
  end

  return verdict
end


wget.callbacks.get_urls = function(file, url, is_css, iri)
  -- progress message
  url_count = url_count + 1
  if url_count % 2 == 0 then
    io.stdout:write("\r - Downloaded "..url_count.." URLs.")
    io.stdout:flush()
  end
end


wget.callbacks.httploop_result = function(url, err, http_stat)
  local sleep_time = 60
  local status_code = http_stat["statcode"]

  if status_code >= 500 then
    io.stdout:write("\nError! (code "..http_stat.statcode.."). Sleeping for ".. sleep_time .." seconds.\n")
    io.stdout:flush()

    -- Note that wget has its own linear backoff to this time as well
    os.execute("sleep " .. sleep_time)
    return wget.actions.CONTINUE
  else
    -- We're okay; sleep a bit (if we have to) and continue
    local sleep_time = 1.0 * (math.random(75, 125) / 100.0)

    if string.match(url["url"], "files.myopera.com%.com") then
      -- We should be able to go fast on images since that's what a web browser does
      -- TODO: Add static.myopera.com (how?)
      sleep_time = 0
    end

    if sleep_time > 0.001 then
      os.execute("sleep " .. sleep_time)
    end

    tries = 0
    return wget.actions.NOTHING
  end
end
