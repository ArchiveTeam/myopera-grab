local url_count = 0


wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  -- Doesn't work for UTF-8 chars:
  -- local username = string.match(start_url_parsed["url"], 'my.opera.com/(.+)/$')
  
  -- Skip closure announcement.
  if url == "http://my.opera.com/chooseopera/blog/2013/10/31/important-announcement-about-your-my-opera-account" then
    return false
  -- Skip common resources (images, CSS, etc)
  elseif string.match(url, "static%.myopera%.com/community") then
    return false
  -- Skip album slideshows
  elseif string.match(url, "/albums/slideshow/") then
    return false
  elseif string.match(url, "%w/xml/%w") then
    return false
  elseif string.match(url, "my%.opera%.com/community/%w") then
    return false
  elseif string.match(url, "blogs%.opera%.com") then
    return false
  elseif string.match(url, "/index%.dml/tag/") then
    return false
  elseif string.match(url, "/archive/monthly/%?") then
    return false
  else
    return verdict
  end
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

    if string.match(url["url"], ".%.myopera%.com") then
      -- We should be able to go fast on images since that's what a web browser does
      sleep_time = 0
    end

    if sleep_time > 0.001 then
      os.execute("sleep " .. sleep_time)
    end

    tries = 0
    return wget.actions.NOTHING
  end
end
