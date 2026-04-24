import { createRouter, createWebHistory } from 'vue-router'
import AdminAuditView from '../views/AdminAuditView.vue'
import AdminCoursesView from '../views/AdminCoursesView.vue'
import AdminLogsView from '../views/AdminLogsView.vue'
import AdminUsersView from '../views/AdminUsersView.vue'
import GraphView from '../views/GraphView.vue'
import LoginView from '../views/LoginView.vue'
import ProfileView from '../views/ProfileView.vue'
import LearningView from '../views/LearningView.vue'
import ResourceDetailView from '../views/ResourceDetailView.vue'
import ResourceListView from '../views/ResourceListView.vue'
import TeacherCoursesView from '../views/TeacherCoursesView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', component: ResourceListView },
    { path: '/resources/:id', component: ResourceDetailView },
    { path: '/graph', component: GraphView },
    { path: '/profile', component: ProfileView },
    { path: '/learning', component: LearningView },
    { path: '/login', component: LoginView },
    { path: '/admin/audit', component: AdminAuditView },
    { path: '/admin/courses', component: AdminCoursesView },
    { path: '/admin/logs', component: AdminLogsView },
    { path: '/admin/users', component: AdminUsersView },
    { path: '/teacher/courses', component: TeacherCoursesView },
  ],
})

router.beforeEach((to) => {
  if (to.path === '/login') return true
  if (to.path === '/' || to.path === '/graph') return true
  const token = localStorage.getItem('token')
  if (!token) return { path: '/login' }
  if (to.path === '/learning') {
    try {
      const user = JSON.parse(localStorage.getItem('user') || 'null')
      const roles = Array.isArray(user?.roles) ? user.roles : []
      if (!roles.includes('student')) return { path: '/' }
    } catch {
      return { path: '/' }
    }
  }
  if (to.path.startsWith('/admin')) {
    try {
      const user = JSON.parse(localStorage.getItem('user') || 'null')
      const roles = Array.isArray(user?.roles) ? user.roles : []
      const isAdmin = roles.includes('admin')
      const isDean = roles.includes('dean')
      if (to.path.startsWith('/admin/logs')) {
        if (!isAdmin) return { path: '/' }
      } else if (to.path.startsWith('/admin/audit')) {
        if (!isDean && !isAdmin) return { path: '/' }
      } else if (to.path.startsWith('/admin/courses')) {
        if (!isDean && !isAdmin) return { path: '/' }
      } else if (to.path.startsWith('/admin/users')) {
        if (!isDean && !isAdmin) return { path: '/' }
      } else {
        if (!isAdmin) return { path: '/' }
      }
    } catch {
      return { path: '/' }
    }
  }
  if (to.path.startsWith('/teacher')) {
    try {
      const user = JSON.parse(localStorage.getItem('user') || 'null')
      const roles = Array.isArray(user?.roles) ? user.roles : []
      const isTeacher = roles.includes('teacher')
      if (!isTeacher) return { path: '/' }
    } catch {
      return { path: '/' }
    }
  }
  return true
})

export default router
