<script setup>
import { computed, onMounted, ref } from 'vue'
import zhCn from 'element-plus/dist/locale/zh-cn.mjs'
import { useRoute, useRouter } from 'vue-router'
import { Collection, Share, User, UserFilled, Setting, Checked, Management, Memo, SwitchButton, Notebook } from '@element-plus/icons-vue'
import logo from './assets/logo.png'

const route = useRoute()
const router = useRouter()

const user = computed(() => {
  try {
    return JSON.parse(localStorage.getItem('user') || '{}')
  } catch {
    return {}
  }
})

const token = computed(() => localStorage.getItem('token') || '')
const isAuthed = computed(() => token.value.length > 0)
const roles = computed(() => Array.isArray(user.value.roles) ? user.value.roles : [])
const elementLocale = zhCn

const isSystemAdmin = computed(() => roles.value.includes('admin'))
const isDean = computed(() => roles.value.includes('dean'))
const isStudent = computed(() => roles.value.includes('student'))
const isTeacherOnly = computed(() => roles.value.includes('teacher') && !roles.value.includes('admin') && !roles.value.includes('dean'))

function go(path) {
  router.push(path)
}

function logout() {
  localStorage.removeItem('token')
  localStorage.removeItem('user')
  router.push('/login')
}
</script>

<template>
  <el-config-provider :locale="elementLocale">
    <el-container class="app-shell">
      <el-aside v-if="route.path !== '/login'" class="app-aside" width="240px">
        <div class="aside-brand" @click="go('/')">
          <img :src="logo" alt="Logo" class="aside-logo" />
          <div class="brand-text">
            <div class="brand-title">教学资源管理系统</div>
            <div class="brand-subtitle">Teaching Resource System</div>
          </div>
        </div>

        <div class="aside-section">
          <div class="section-title">导航</div>
          <el-menu
            mode="vertical"
            :default-active="route.path"
            class="side-menu"
            :ellipsis="false"
            @select="go"
          >
            <el-menu-item index="/">
              <el-icon><Collection /></el-icon>
              <span>资源中心</span>
            </el-menu-item>
            <el-menu-item index="/graph">
              <el-icon><Share /></el-icon>
              <span>知识图谱</span>
            </el-menu-item>
            <el-menu-item v-if="isStudent" index="/learning">
              <el-icon><Notebook /></el-icon>
              <span>学习推荐</span>
            </el-menu-item>
            <el-menu-item v-if="isTeacherOnly" index="/teacher/courses">
              <el-icon><Memo /></el-icon>
              <span>课程管理</span>
            </el-menu-item>
            <el-menu-item v-if="isDean" index="/admin/audit">
              <el-icon><Checked /></el-icon>
              <span>资源审核</span>
            </el-menu-item>
            <el-menu-item v-if="isDean || isSystemAdmin" index="/admin/courses">
              <el-icon><Management /></el-icon>
              <span>课程分配</span>
            </el-menu-item>
            <el-menu-item v-if="isSystemAdmin" index="/admin/logs">
              <el-icon><Setting /></el-icon>
              <span>系统日志</span>
            </el-menu-item>
            <el-menu-item v-if="isSystemAdmin" index="/admin/users">
              <el-icon><User /></el-icon>
              <span>账号权限</span>
            </el-menu-item>
            <el-menu-item v-else-if="isDean" index="/admin/users">
              <el-icon><User /></el-icon>
              <span>账号管理</span>
            </el-menu-item>
          </el-menu>
        </div>

        <div class="aside-footer">
          <template v-if="isAuthed">
            <div class="user-card" @click="go('/profile')">
              <el-avatar :size="38" :icon="UserFilled" />
              <div class="user-meta">
                <div class="username">{{ user.username || '用户' }}</div>
                <div class="role-text">{{ roles.join(' / ') || '未分配角色' }}</div>
              </div>
            </div>
            <el-button class="logout-btn" plain @click="logout">
              <el-icon><SwitchButton /></el-icon>
              退出登录
            </el-button>
          </template>
          <el-button v-else class="login-btn" type="primary" @click="go('/login')">登录</el-button>
        </div>
      </el-aside>

      <el-container class="app-content">
        <el-main class="app-main">
          <div class="page-shell">
            <router-view />
          </div>
        </el-main>
      </el-container>
    </el-container>
  </el-config-provider>
</template>

<style>
:deep(.el-pagination) {
  justify-content: center;
  margin-top: 18px;
}

:deep(.el-pagination__sizes),
:deep(.el-pagination__jump) {
  display: flex;
  align-items: center;
}

:deep(.el-pagination__total),
:deep(.el-pagination__jump),
:deep(.el-pagination__sizes .el-select) {
  color: var(--app-text-secondary);
}

:deep(.el-pagination button),
:deep(.el-pagination .el-pager li) {
  font-weight: 600;
}

:root {
  --app-bg: #eef3fb;
  --app-surface: rgba(255, 255, 255, 0.82);
  --app-surface-strong: #ffffff;
  --app-border: rgba(36, 60, 96, 0.10);
  --app-text: #1f2a44;
  --app-text-secondary: #667085;
  --app-shadow: 0 18px 48px rgba(39, 65, 118, 0.12);
  --app-radius-lg: 20px;
  --app-radius-md: 14px;
  --app-radius-sm: 10px;
  --app-sidebar-bg: #b8d0ff;
  --app-sidebar-hover-bg: rgba(255, 255, 255, 0.26);
  --app-sidebar-active-bg: rgba(255, 255, 255, 0.44);
  --app-sidebar-text: #1d2b4d;
}

html, body, #app {
  margin: 0;
  min-height: 100%;
  background:
    radial-gradient(1200px 700px at 0% 0%, rgba(115, 144, 255, 0.18), transparent 55%),
    radial-gradient(900px 500px at 100% 0%, rgba(76, 110, 245, 0.12), transparent 45%),
    linear-gradient(180deg, #eef3fb 0%, #e7edf8 100%);
  color: var(--app-text);
  font-family: Inter, 'PingFang SC', 'Microsoft YaHei', system-ui, sans-serif;
}

body {
  min-height: 100vh;
}

.app-shell {
  min-height: 100vh;
  width: 100vw;
}

.app-aside {
  background: var(--app-sidebar-bg);
  color: var(--app-sidebar-text);
  box-shadow: 8px 0 32px rgba(58, 98, 166, 0.12);
  border-right: 1px solid rgba(29, 43, 77, 0.08);
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.aside-brand {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 22px 18px 18px;
  cursor: pointer;
  border-bottom: 1px solid rgba(29, 43, 77, 0.08);
}

.aside-logo {
  width: 46px;
  height: 46px;
  object-fit: contain;
  border-radius: 14px;
  background: rgba(255,255,255,0.35);
  box-shadow: 0 10px 24px rgba(29, 43, 77, 0.10);
}

.brand-text {
  min-width: 0;
}

.brand-title {
  font-size: 1rem;
  font-weight: 800;
  letter-spacing: -0.02em;
  line-height: 1.2;
}

.brand-subtitle {
  margin-top: 4px;
  font-size: 12px;
  opacity: 0.72;
}

.aside-section {
  padding: 16px 10px 12px;
  flex: 1;
  min-height: 0;
}

.section-title {
  font-size: 12px;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  opacity: 0.72;
  margin: 4px 10px 10px;
}

.side-menu {
  border-right: none !important;
  background: transparent !important;
  --el-menu-bg-color: transparent;
  --el-menu-text-color: var(--app-sidebar-text);
  --el-menu-hover-text-color: var(--app-sidebar-text);
  --el-menu-active-color: var(--app-sidebar-text);
  --el-menu-hover-bg-color: var(--app-sidebar-hover-bg);
}

.side-menu .el-menu-item {
  height: 46px;
  margin: 6px 0;
  border-radius: 12px;
  font-weight: 600;
}

.side-menu .el-menu-item.is-active {
  background: var(--app-sidebar-active-bg) !important;
}

.aside-footer {
  padding: 14px 14px 18px;
  border-top: 1px solid rgba(29, 43, 77, 0.08);
  display: grid;
  gap: 10px;
}

.user-card {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 12px;
  border-radius: 16px;
  background: rgba(255,255,255,0.34);
  cursor: pointer;
  transition: transform 0.2s ease, background 0.2s ease;
}

.user-card:hover {
  transform: translateY(-1px);
  background: rgba(255,255,255,0.46);
}

.user-meta {
  min-width: 0;
}

.user-meta .username {
  font-weight: 700;
  color: var(--app-text);
}

.role-text {
  font-size: 12px;
  color: var(--app-text-secondary);
  margin-top: 2px;
}

.login-btn, .logout-btn {
  width: 100%;
  border-radius: 12px;
  font-weight: 600;
}

.app-content {
  min-width: 0;
  flex: 1;
}

.app-main {
  width: 100%;
  padding: 28px;
}

.page-shell {
  width: 100%;
  max-width: none;
  margin: 0;
}

/* Global page refinements */
.main-card, .el-card {
  border-radius: var(--app-radius-lg) !important;
  border: 1px solid var(--app-border) !important;
  background: var(--app-surface) !important;
  box-shadow: var(--app-shadow);
  backdrop-filter: blur(14px);
  -webkit-backdrop-filter: blur(14px);
}

.main-card {
  width: 100%;
}

.el-table {
  --el-table-border-color: rgba(84, 112, 180, 0.14);
  --el-table-header-bg-color: rgba(241, 246, 255, 0.96);
  --el-table-row-hover-bg-color: rgba(184, 208, 255, 0.14);
  --el-table-text-color: var(--app-text);
  border-radius: 14px;
  overflow: hidden;
}

.el-button.is-round,
.el-input__wrapper,
.el-select__wrapper,
.el-textarea__inner,
.el-dialog,
.el-message-box,
.el-drawer {
  border-radius: 12px !important;
}

.el-menu-item, .el-sub-menu__title {
  transition: all 0.2s ease;
}

.el-menu-item:hover {
  transform: translateX(2px);
}

.page-view {
  padding: 0;
  width: 100%;
}

.page-toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  margin-bottom: 18px;
}

.page-toolbar__title {
  display: flex;
  align-items: center;
  gap: 12px;
}

.title-icon {
  width: 42px;
  height: 42px;
  border-radius: 14px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  background: linear-gradient(135deg, rgba(95, 129, 255, 0.16), rgba(61, 95, 255, 0.06));
  color: #4564f5;
}

.title-block {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.title {
  font-size: 1.15rem;
  font-weight: 800;
  color: var(--app-text);
}

.title-hint {
  color: var(--app-text-secondary);
  font-size: 0.875rem;
}

@media (max-width: 1024px) {
  .app-shell {
    flex-direction: column;
  }

  .app-aside {
    width: 100% !important;
  }

  .aside-section {
    padding-bottom: 6px;
  }

  .app-main {
    padding: 16px;
  }
}
</style>
