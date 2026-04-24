<script setup>
import { computed, onMounted, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { 
  Search, 
  Plus, 
  Refresh, 
  Delete, 
  Management, 
  User, 
  Collection,
  Warning,
  Edit
} from '@element-plus/icons-vue'
import api from '../api/client'

const loading = ref(false)
const courses = ref([])
const teachers = ref([])
const assignedByCourseId = ref({})
const keyword = ref('')
const deletingId = ref(null)

const rolesOfMe = computed(() => {
  try {
    const u = JSON.parse(localStorage.getItem('user') || 'null')
    return Array.isArray(u?.roles) ? u.roles : []
  } catch {
    return []
  }
})
const isDean = computed(() => rolesOfMe.value.includes('dean') || rolesOfMe.value.includes('admin'))

const createOpen = ref(false)
const createForm = ref({ name: '', code: '', department: '', description: '' })

const assignOpen = ref(false)
const assignLoading = ref(false)
const assignCourse = ref(null)
const assignments = ref([{ teacher_id: null, class_name: '' }])

const teacherOptions = computed(() => (teachers.value || []).map((t) => ({ label: `${t.name}${t.has_user ? '' : ' (未绑定账号)'}`, value: t.id })))

async function fetchCourses() {
  loading.value = true
  try {
    const resp = await api.get('/api/courses', { params: { q: (keyword.value || '').trim() || undefined } })
    courses.value = resp.data.items || []
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '加载课程失败')
    courses.value = []
  } finally {
    loading.value = false
  }
}

async function fetchTeachers() {
  try {
    const resp = await api.get('/api/teachers')
    teachers.value = resp.data.items || []
  } catch (e) {
    teachers.value = []
  }
}

async function fetchAssigned(courseId) {
  const resp = await api.get(`/api/courses/${courseId}/teachers`)
  return resp.data.items || []
}

async function refreshAssignedMap() {
  const map = {}
  const list = courses.value || []
  await Promise.all(
    list.map(async (c) => {
      try {
        map[c.id] = await fetchAssigned(c.id)
      } catch {
        map[c.id] = []
      }
    }),
  )
  assignedByCourseId.value = map
}

function openCreate() {
  createForm.value = { id: null, name: '', code: '', department: '', description: '' }
  createOpen.value = true
}

function openEdit(row) {
  createForm.value = { 
    id: row.id, 
    name: row.name || '', 
    code: row.code || '', 
    department: row.department || '', 
    description: row.description || '' 
  }
  createOpen.value = true
}

async function submitCreate() {
  const name = (createForm.value.name || '').trim()
  const code = (createForm.value.code || '').trim()
  const department = (createForm.value.department || '').trim()
  const description = (createForm.value.description || '').trim()
  if (!name) {
    ElMessage.warning('请输入课程名称')
    return
  }
  try {
    await api.post('/api/courses', { 
      name, 
      code: code || undefined, 
      department: department || undefined,
      description: description || undefined 
    })
    ElMessage.success('课程创建/更新成功')
    createOpen.value = false
    await fetchCourses()
    await refreshAssignedMap()
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '操作失败')
  }
}

async function openAssign(row) {
  assignCourse.value = row
  assignOpen.value = true
  assignLoading.value = true
  try {
    const items = await fetchAssigned(row.id)
    if (items && items.length > 0) {
      assignments.value = items.map(t => ({ teacher_id: t.id, class_name: t.class_name || '' }))
    } else {
      assignments.value = [{ teacher_id: null, class_name: '' }]
    }
  } catch (e) {
    assignments.value = [{ teacher_id: null, class_name: '' }]
    ElMessage.error(e?.response?.data?.error?.message || '加载已分配教师失败')
  } finally {
    assignLoading.value = false
  }
}

function addAssignment() {
  assignments.value.push({ teacher_id: null, class_name: '' })
}

function removeAssignment(index) {
  assignments.value.splice(index, 1)
  if (assignments.value.length === 0) {
    addAssignment()
  }
}

async function submitAssign() {
  const course = assignCourse.value
  if (!course?.id) return
  assignLoading.value = true
  try {
    const validAssignments = assignments.value.filter(a => a.teacher_id)
    await api.put(`/api/courses/${course.id}/teachers`, { assignments: validAssignments })
    ElMessage.success('教学分配保存成功')
    assignOpen.value = false
    const items = await fetchAssigned(course.id)
    assignedByCourseId.value = { ...(assignedByCourseId.value || {}), [course.id]: items }
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '保存失败')
  } finally {
    assignLoading.value = false
  }
}

async function removeCourse(row) {
  try {
    await ElMessageBox.confirm(
      `确认强制删除课程「${row.name}」？此操作将同步删除该课程下的所有知识点、资料及其关联关系，且不可恢复。`,
      '危险操作提示',
      { 
        type: 'warning', 
        confirmButtonText: '强制删除', 
        cancelButtonText: '取消',
        confirmButtonClass: 'el-button--danger'
      },
    )
  } catch {
    return
  }
  deletingId.value = row.id
  try {
    const resp = await api.delete(`/api/courses/${row.id}`, { params: { force: 1 } })
    const d = resp.data?.deleted || {}
    ElMessage.success(`清理完成（课程:${d.course || 0} 知识点:${d.knowledge_points || 0} 资料:${d.resources || 0} 关联:${d.course_teachers || 0}）`)
    await fetchCourses()
    await refreshAssignedMap()
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '删除失败')
  } finally {
    deletingId.value = null
  }
}

onMounted(async () => {
  await Promise.all([fetchCourses(), fetchTeachers()])
  await refreshAssignedMap()
})
</script>

<template>
  <div class="admin-courses-container">
    <el-card class="main-card" shadow="never">
      <template #header>
        <div class="card-header">
          <div class="title-section">
            <el-icon><Management /></el-icon>
            <span>课程管理与教学分配</span>
          </div>
          <div class="header-tools">
            <el-input 
              v-model="keyword" 
              clearable 
              placeholder="按名称/编码检索" 
              style="width: 240px" 
              :prefix-icon="Search"
              @keyup.enter="async () => { await fetchCourses(); await refreshAssignedMap() }" 
            />
            <el-button type="primary" :icon="Plus" @click="openCreate">新增课程</el-button>
          </div>
        </div>
      </template>

      <el-alert v-if="!isDean" title="权限受限" type="warning" description="当前页面仅教务管理员可进行修改操作。" show-icon :closable="false" style="margin-bottom: 20px" />

      <el-table :data="courses" v-loading="loading" border stripe style="width: 100%" class="data-table">
        <el-table-column prop="name" label="课程信息" min-width="200">
          <template #default="{ row }">
            <div class="course-info-cell">
              <el-icon><Collection /></el-icon>
              <div class="course-text">
                <div class="course-name">{{ row.name }}</div>
                <div class="course-code">编码: {{ row.code || '无' }} | 系: {{ row.department || '未指定' }}</div>
              </div>
            </div>
          </template>
        </el-table-column>
        <el-table-column prop="description" label="课程描述" min-width="240" show-overflow-tooltip />
        <el-table-column label="已分配教学任务" min-width="280">
          <template #default="{ row }">
            <div class="assignment-tags">
              <el-tooltip 
                v-for="t in (assignedByCourseId[row.id] || [])" 
                :key="t.id" 
                :content="`教师: ${t.name} | 班级: ${t.class_name || '未指定'}`" 
                placement="top"
              >
                <el-tag size="small" effect="plain" class="assign-tag">
                  <el-icon><User /></el-icon>
                  {{ t.name }}{{ t.class_name ? ` (${t.class_name})` : '' }}
                </el-tag>
              </el-tooltip>
              <span v-if="!((assignedByCourseId[row.id] || []).length)" class="empty-text">未分配教师</span>
            </div>
          </template>
        </el-table-column>
        <el-table-column prop="created_at" label="创建日期" width="160">
          <template #default="{ row }">
            {{ row.created_at ? new Date(row.created_at).toLocaleDateString() : '-' }}
          </template>
        </el-table-column>
        <el-table-column label="操作" width="260" fixed="right" align="center">
          <template #default="{ row }">
            <el-button-group>
              <el-button size="small" :icon="Edit" @click="openEdit(row)">编辑</el-button>
              <el-button size="small" type="primary" :icon="User" @click="openAssign(row)">分配任务</el-button>
              <el-button size="small" type="danger" :icon="Delete" :loading="deletingId === row.id" @click="removeCourse(row)"></el-button>
            </el-button-group>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <!-- Create Course Dialog -->
    <el-dialog v-model="createOpen" :title="createForm.id ? '编辑课程' : '新增课程'" width="480px">
      <el-form :model="createForm" label-position="top">
        <el-form-item label="课程名称" required>
          <el-input v-model="createForm.name" placeholder="如: 高等数学、数据结构" />
        </el-form-item>
        <el-form-item label="课程编码">
          <el-input v-model="createForm.code" placeholder="如: MATH-101" />
        </el-form-item>
        <el-form-item label="所属院系">
          <el-input v-model="createForm.department" placeholder="支持多个院系，用逗号分隔（如: 计算机系, 数学系）" />
        </el-form-item>
        <el-form-item label="课程简述">
          <el-input v-model="createForm.description" type="textarea" :rows="3" placeholder="简要介绍课程教学内容..." />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="createOpen = false">取消</el-button>
        <el-button type="primary" @click="submitCreate">确认保存</el-button>
      </template>
    </el-dialog>

    <!-- Assign Teachers Dialog -->
    <el-dialog v-model="assignOpen" title="分配教学任务" width="600px" destroy-on-close>
      <div class="assign-dialog-content">
        <div class="course-header">
          <el-icon><Collection /></el-icon>
          <span>正在为课程 <strong>{{ assignCourse?.name }}</strong> 分配教师</span>
        </div>
        
        <el-form label-position="top">
          <div v-for="(item, index) in assignments" :key="index" class="assign-row">
            <div class="row-inputs">
              <el-select v-model="item.teacher_id" filterable placeholder="选择任课教师" style="flex: 1.5">
                <el-option v-for="opt in teacherOptions" :key="opt.value" :label="opt.label" :value="opt.value" />
              </el-select>
              <el-input v-model="item.class_name" placeholder="教授班级 (如: 计科2101)" style="flex: 1" />
            </div>
            <el-button type="danger" circle icon="Delete" @click="removeAssignment(index)" :disabled="assignments.length <= 1" />
          </div>
          
          <el-button type="primary" plain style="width: 100%; margin-top: 10px" @click="addAssignment" :icon="Plus">
            新增任课配置
          </el-button>
        </el-form>
      </div>
      <template #footer>
        <el-button @click="assignOpen = false">取消</el-button>
        <el-button type="primary" :loading="assignLoading" @click="submitAssign">保存配置</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<style scoped>
.admin-courses-container {
  padding: 0;
}

.main-card {
  border-radius: 12px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.title-section {
  display: flex;
  align-items: center;
  gap: 10px;
  font-weight: 600;
  font-size: 18px;
  color: #303133;
}

.header-tools {
  display: flex;
  gap: 12px;
}

.data-table {
  border-radius: 8px;
  overflow: hidden;
}

.course-info-cell {
  display: flex;
  align-items: center;
  gap: 12px;
}

.course-info-cell .el-icon {
  color: #409eff;
  font-size: 20px;
}

.course-name {
  font-weight: 600;
  color: #303133;
}

.course-code {
  font-size: 12px;
  color: #909399;
  margin-top: 2px;
}

.assignment-tags {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.assign-tag {
  display: flex;
  align-items: center;
  gap: 4px;
}

.empty-text {
  font-size: 13px;
  color: #c0c4cc;
  font-style: italic;
}

.assign-dialog-content {
  padding: 0 10px;
}

.course-header {
  background: #f0f7ff;
  padding: 12px 16px;
  border-radius: 8px;
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 24px;
  color: #409eff;
}

.assign-row {
  display: flex;
  gap: 12px;
  align-items: center;
  margin-bottom: 16px;
}

.row-inputs {
  flex: 1;
  display: flex;
  gap: 12px;
}

:deep(.el-table__header) {
  background-color: #f5f7fa;
}
</style>
