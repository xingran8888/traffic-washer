#!/usr/bin/env python3
"""Add master toggle for advanced settings"""
f = '/home/ghss/traffic-washer/app.py'
c = open(f).read()

# 1. Add adv_enabled global
c = c.replace(
    'speed_test_on_start = True   # v5.3: 启动时自动测速',
    'speed_test_on_start = True   # v5.3: 启动时自动测速\nadv_enabled = False           # v5.4: 高级设置总开关'
)

# 2. Add toggle switch in HTML - replace the collapsible div
old_collapsible = '<div class="collapsible" onclick="this.classList.toggle(\'open\');this.nextElementSibling.classList.toggle(\'show\')">高级设置</div>'
new_collapsible = '''<div style="display:flex;justify-content:space-between;align-items:center;padding:12px 0;border-bottom:1px solid #eee">
          <span style="font-size:14.5px;font-weight:600;color:#333">高级设置</span>
          <label style="display:flex;align-items:center;gap:8px;cursor:pointer">
            <input type="checkbox" name="adv_enabled" {{ 'checked' if adv_enabled else '' }} onchange="document.getElementById('adv-content').style.display=this.checked?'block':'none'" style="width:18px;height:18px">
            <span style="font-size:12px;color:#666">{{ '已开启' if adv_enabled else '已关闭' }}</span>
          </label>
        </div>
        <div id="adv-content" class="collapse-content {{ 'show' if adv_enabled else '' }}" style="display:{{ 'block' if adv_enabled else 'none' }}">'''
c = c.replace(old_collapsible, new_collapsible)

# 3. Save adv_enabled in setconfig
old_setconfig_save = '        speed_test_on_start = request.form.get("speed_test") == "on"'
new_setconfig_save = '''        adv_enabled = request.form.get("adv_enabled") == "on"
        speed_test_on_start = request.form.get("speed_test") == "on"'''
c = c.replace(old_setconfig_save, new_setconfig_save)

# 4. Pass adv_enabled to template
c = c.replace(
    'speed_test_on_start=speed_test_on_start, min_speed_mbps=min_speed_mbps,',
    'adv_enabled=adv_enabled, speed_test_on_start=speed_test_on_start, min_speed_mbps=min_speed_mbps,'
)

open(f, 'w').write(c)
print(f"OK: {c.count(chr(10))+1} lines")
