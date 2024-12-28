local debug = 0
return {
  filter = function(ctx)
    local hasSubstring = string.find(string.lower(ctx.post.text), "amogus") ~= nil
    if debug == 1 then
      print('== POST')
      for key, value in pairs(ctx.post) do
          print(key, value)
      end

      print('== YOU FOLLOW')
      for key, value in pairs(ctx.follows) do
          print(key, value)
      end

      print('== THEY FOLLOW')
      for key, value in pairs(ctx.followed) do
          print(key, value)
      end
    end
    return hasSubstring
  end,
}
